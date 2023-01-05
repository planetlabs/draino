package drain

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/kubernetes/utils"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// NodeRetryStrategyAnnotation annotation used to override the retry strategy on a node
	NodeRetryStrategyAnnotation string = "draino/retry-strategy"
	// RetryWallConditionType the condition type used to save the drain failure count
	RetryWallConditionType corev1.NodeConditionType = "DrainFailure"
	// retryConditionMsgSeparator the separator used in the condition message to separate the count from the message
	retryConditionMsgSeparator string = "|"
)

// TimeZero is used as default if there was no retry set
var TimeZero = time.Time{}

type RetryWall interface {
	// SetNewRetryWallTimestamp increases the retry count on the given node with the given string
	// now is used for last heartbeat timestamp and should usually be time.Now()
	SetNewRetryWallTimestamp(ctx context.Context, node *corev1.Node, reason string, now time.Time) (*corev1.Node, error)
	// GetRetryWallTimestamp returns the next time the given node should be retried
	// If there was no drain failure yet, it will return a zero timestamp
	GetRetryWallTimestamp(*corev1.Node) time.Time
	// GetDrainRetryAttemptsCount returns the amount of drain failures recorded for the given node
	GetDrainRetryAttemptsCount(*corev1.Node) int
	// ResetRetryCount will reset the retry count of the given node to zero
	ResetRetryCount(context.Context, *corev1.Node) (*corev1.Node, error)
	// IsAboveAlertingThreshold returns true if the node is above alerting threshold of the corresponding retry strategy
	IsAboveAlertingThreshold(*corev1.Node) bool
}

type retryWallImpl struct {
	logger logr.Logger
	client client.Client
	// the default strategy is the first one passed to RegisterRetryStrategies
	// it's also available in the strategies map
	defaultStrategy RetryStrategy
	strategies      map[string]RetryStrategy
}

var _ RetryWall = &retryWallImpl{}

// NewRetryWall will return a new instance of the retry wall
// It will return an error if no strategy was given
func NewRetryWall(client client.Client, logger logr.Logger, strategies ...RetryStrategy) (RetryWall, error) {
	if len(strategies) == 0 {
		return nil, fmt.Errorf("please provide at least one retry strategy to the retry wall, otherwise it will not work.")
	}

	wall := &retryWallImpl{
		client:     client,
		logger:     logger.WithName("retry-wall"),
		strategies: map[string]RetryStrategy{},
	}
	wall.registerRetryStrategies(strategies...)

	return wall, nil
}

func (wall *retryWallImpl) registerRetryStrategies(strategies ...RetryStrategy) {
	if len(strategies) == 0 {
		return
	}
	if wall.defaultStrategy == nil {
		wall.defaultStrategy = strategies[0]
	}
	for _, strategy := range strategies {
		wall.strategies[strategy.GetName()] = strategy
	}
}

func (wall *retryWallImpl) SetNewRetryWallTimestamp(ctx context.Context, node *corev1.Node, reason string, now time.Time) (*corev1.Node, error) {
	retryCount, _, err := wall.getRetry(node)
	if err != nil {
		wall.logger.Error(err, "unable to get retry wall count from node", "node", node.GetName(), "conditions", node.Status.Conditions)
		retryCount = 0
	}

	retryCount += 1
	return wall.patchRetryCountOnNode(ctx, node, retryCount, reason, now)
}

func (wall *retryWallImpl) GetRetryWallTimestamp(node *corev1.Node) time.Time {
	retries, lastHeartbeatTime, err := wall.getRetry(node)
	if err != nil {
		wall.logger.Error(err, "unable to get retry wall information from node", "node", node.GetName(), "conditions", node.Status.Conditions)
		return TimeZero
	}

	// if this is the first try, we should not inject any delay
	if retries == 0 {
		return TimeZero
	}

	delay := wall.getRetryDelay(node, retries)
	return lastHeartbeatTime.Add(delay)
}

func (wall *retryWallImpl) getRetryDelay(node *corev1.Node, retries int) time.Duration {
	strategy := wall.getStrategyFromNode(node)
	if retries >= strategy.GetAlertThreashold() {
		wall.logger.Info("retry wall is hitting limit for node", "node_name", node.GetName(), "retry_strategy", strategy.GetName(), "retries", retries, "max_retries", strategy.GetAlertThreashold())
	}

	return strategy.GetDelay(retries)
}

func (wall *retryWallImpl) GetDrainRetryAttemptsCount(node *corev1.Node) int {
	retryCount, _, err := wall.getRetry(node)
	if err != nil {
		wall.logger.Error(err, "unable to get retry wall count from node", "node", node.GetName(), "conditions", node.Status.Conditions)
		return 0
	}
	return retryCount
}

func (wall *retryWallImpl) IsAboveAlertingThreshold(node *corev1.Node) bool {
	retries, _, err := wall.getRetry(node)
	if err != nil {
		return false
	}
	strategy := wall.getStrategyFromNode(node)
	return retries >= strategy.GetAlertThreashold()
}

func (wall *retryWallImpl) getStrategyFromNode(node *corev1.Node) RetryStrategy {
	defaultStrategy := wall.defaultStrategy

	if val, ok := node.Annotations[NodeRetryStrategyAnnotation]; ok {
		strategy, ok := wall.strategies[val]
		if ok {
			defaultStrategy = strategy
		} else {
			wall.logger.Error(fmt.Errorf("cannot find node strategy '%s' in retry wall", val), "falling back to default retry strategy", "node_name", node.GetName(), "strategy", val, "default_strategy", defaultStrategy.GetName())
		}
	}

	nodeAnnotationStrategy, err := buildNodeAnnotationRetryStrategy(node, defaultStrategy)
	if err != nil {
		wall.logger.Error(err, "node contains invalid retry wall configruation", "node_name", node.GetName(), "annotations", node.GetAnnotations())
	}

	return nodeAnnotationStrategy
}

func (wall *retryWallImpl) ResetRetryCount(ctx context.Context, node *corev1.Node) (*corev1.Node, error) {
	return wall.patchRetryCountOnNode(ctx, node, 0, "Retry count was reset to zero", time.Now())
}

func (wall *retryWallImpl) getRetry(node *corev1.Node) (int, time.Time, error) {
	_, condition, found := utils.FindNodeCondition(RetryWallConditionType, node)
	if !found {
		return 0, TimeZero, nil
	}

	retryCount, err := unserializeConditionMessage(condition.Message)
	if err != nil {
		return 0, TimeZero, err
	}

	return retryCount, condition.LastHeartbeatTime.Time, nil
}

func (wall *retryWallImpl) patchRetryCountOnNode(ctx context.Context, node *corev1.Node, retryCount int, reason string, now time.Time) (*corev1.Node, error) {
	newNode := node.DeepCopy()
	pos, _, found := utils.FindNodeCondition(RetryWallConditionType, newNode)
	if !found {
		// we can use the length as the index as the append is done afterwards
		pos = len(newNode.Status.Conditions)
		newNode.Status.Conditions = append(newNode.Status.Conditions, corev1.NodeCondition{
			Type:               RetryWallConditionType,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: metav1.NewTime(now),
			Reason:             "DrainRetryWall",
		})
	}

	delay := wall.getRetryDelay(node, retryCount)
	blockedUntil := now.Add(delay)

	newNode.Status.Conditions[pos].LastHeartbeatTime = metav1.NewTime(now)
	newNode.Status.Conditions[pos].Message = serializeConditionMessage(retryCount, blockedUntil, reason)

	err := wall.
		client.
		Status().
		Patch(ctx, newNode, &NodeConditionPatch{ConditionType: RetryWallConditionType})

	return newNode, err
}

func serializeConditionMessage(retryCount int, blockedUntil time.Time, reason string) string {
	blockedUntilStr := blockedUntil.Format(time.RFC822)
	return fmt.Sprintf("%d%s%s%s%s", retryCount, retryConditionMsgSeparator, blockedUntilStr, retryConditionMsgSeparator, reason)
}

func unserializeConditionMessage(message string) (retryCount int, returnErr error) {
	split := strings.Split(message, retryConditionMsgSeparator)
	if len(split) != 3 {
		returnErr = fmt.Errorf("invalid formatted node drain retry condition message: %s", message)
		return
	}

	val, err := strconv.ParseInt(split[0], 10, 32)
	if err != nil {
		returnErr = fmt.Errorf("cannot parse retry count from node condition: %v", err)
		return
	}

	return int(val), nil
}
