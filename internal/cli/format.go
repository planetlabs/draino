package cli

type outputFormatType string

func (o *outputFormatType) String() string {
	if *o != "" {
		return string(*o)
	} else {
		return string(formatTable)
	}
}
func (o *outputFormatType) Set(s string) error { *o = outputFormatType(s); return nil }
func (o *outputFormatType) Type() string       { return "outputFormatType" }

const (
	formatJSON  outputFormatType = "json"
	formatTable outputFormatType = "table"
)
