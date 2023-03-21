package cli

type OutputFormatType string

func (o *OutputFormatType) String() string {
	if *o != "" {
		return string(*o)
	} else {
		return string(FormatTable)
	}
}
func (o *OutputFormatType) Set(s string) error { *o = OutputFormatType(s); return nil }
func (o *OutputFormatType) Type() string       { return "outputFormatType" }

const (
	FormatJSON  OutputFormatType = "json"
	FormatTable OutputFormatType = "table"
)
