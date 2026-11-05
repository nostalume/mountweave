package s3

import (
	"errors"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	core "github.com/nostalume/mountweave"
)

func evidence(tag, version *string) core.Evidence {
	var e core.Evidence
	if value := aws.ToString(tag); value != "" {
		e.Validator = core.Validator{Domain: "s3-etag", Tag: value}
	}
	if value := aws.ToString(version); value != "null" {
		e.Version = core.Version(value)
	}
	return e
}

// Only supported native predicates enter requests. A tag list is never reduced
// to one arbitrary member, and conditional conflicts never refresh expectations.
func nativeCondition(c core.Condition, reading bool) (match, none *string, err error) {
	if err = c.ForDomain("s3-etag"); err != nil {
		return
	}
	a, b := c.Match(), c.NotMatch()
	if a != "" {
		if a != "*" && !(core.Validator{Tag: a}).Valid() {
			err = core.Fail(core.UnsupportedOperation, nil)
			return
		}
		if !reading && (a == "*" || b != "") {
			err = core.Fail(core.UnsupportedOperation, nil)
			return
		}
		if !reading && strings.HasPrefix(a, "W/") {
			err = core.Fail(core.PreconditionFailed, &core.ConditionFailure{})
			return
		}
		match = aws.String(a)
	}
	if b != "" {
		if !reading && b != "*" || b != "*" && !(core.Validator{Tag: b}).Valid() {
			err = core.Fail(core.UnsupportedOperation, nil)
			return
		}
		none = aws.String(strings.TrimPrefix(b, "W/"))
	}
	return
}

func readFailure(err error) error {
	var response *smithyhttp.ResponseError
	if errors.As(err, &response) && (response.HTTPStatusCode() == 304 || response.HTTPStatusCode() == 412) {
		tag := response.Response.Header.Get("ETag")
		return core.Fail(core.PreconditionFailed, errors.Join(&core.ConditionFailure{Negative: response.HTTPStatusCode() == 304, Evidence: evidence(&tag, nil)}, err))
	}
	return translate(err)
}
