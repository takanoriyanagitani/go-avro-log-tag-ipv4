package tagip4

import (
	"errors"
	"fmt"
	"iter"
	"net/netip"
	"regexp"
)

var (
	ErrInvalidIp error = errors.New("invalid ip")

	ErrInvalidBody error = errors.New("invalid body")
	ErrInvalidTags error = errors.New("invalid tags")
)

type Ip4 [4]byte

func (i Ip4) AsBytes() []byte { return i[:] }

func (i Ip4) String() string {
	var addr netip.Addr = netip.AddrFrom4(i)
	return addr.String()
}

type Original map[string]any

type IpSourceColumnName string

const IpSourceColumnNameDefault IpSourceColumnName = "body"

type ExtractIpLike func(string) (ipLike []string)

type ExtractPattern struct{ *regexp.Regexp }

func (p ExtractPattern) BodyToIps(body string) (ipLike []string) {
	return p.Regexp.FindAllString(body, -1)
}

func (p ExtractPattern) AsExtractIp() ExtractIpLike { return p.BodyToIps }

var ExtractPatternDefault ExtractPattern = ExtractPattern{
	Regexp: regexp.MustCompile(`\b(\d{1,3}\.){3}\d{1,3}\b`),
}

var ExtractIpLikeDefault ExtractIpLike = ExtractPatternDefault.AsExtractIp()

type ParseIp func(string) (Ip4, error)

var StringToAddr func(string) (netip.Addr, error) = netip.ParseAddr

func AddrToIpv4(a netip.Addr) (Ip4, error) {
	var ret Ip4
	if !a.Is4() {
		return ret, ErrInvalidIp
	}

	copy(ret[:], a.AsSlice())
	return ret, nil
}

var ParseIpDefault ParseIp = ComposeErr(StringToAddr, AddrToIpv4)

type AddExtractedIp func([]Ip4, string) []Ip4

func (e ExtractIpLike) ToAddExtractedIp(parser ParseIp) AddExtractedIp {
	return func(original []Ip4, body string) []Ip4 {
		var ipLike []string = e(body)
		var ret []Ip4 = original
		for _, ip := range ipLike {
			parsed, e := parser(ip)
			if nil == e {
				ret = append(ret, parsed)
			}
		}
		return ret
	}
}

var AddExtractedIpDefault AddExtractedIp = ExtractIpLikeDefault.
	ToAddExtractedIp(ParseIpDefault)

type TagColumnName string

const TagColumnNameDefault TagColumnName = "tags"

func AddToTag(tags []string, ips []Ip4) []string {
	for _, ip := range ips {
		var s string = ip.String()
		tags = append(tags, s)
	}
	return tags
}

type Config struct {
	IpSourceColumnName
	TagColumnName
}

var ConfigDefault Config = Config{
	IpSourceColumnName: IpSourceColumnNameDefault,
	TagColumnName:      TagColumnNameDefault,
}

func (c Config) GetBody(original map[string]any) (body string, e error) {
	var abody any = original[string(c.IpSourceColumnName)]
	switch typ := abody.(type) {
	case nil:
		return "", nil
	case string:
		return typ, nil
	default:
		return "", ErrInvalidBody
	}
}

func (c Config) GetTags(
	buf []string,
	original map[string]any,
) (tags []string, e error) {
	buf = buf[:0]

	var atags any = original[string(c.TagColumnName)]
	switch typ := atags.(type) {
	case nil:
		return nil, nil
	case []string:
		buf = append(buf, typ...)
		return buf, nil
	case []any:
		for _, a := range typ {
			switch s := a.(type) {
			case string:
				buf = append(buf, s)
			default:
			}
		}
		return buf, nil
	default:
		return nil, fmt.Errorf("%w: %v", ErrInvalidTags, typ)
	}
}

type Converter struct {
	Config
	AddExtractedIp
}

var ConverterDefault Converter = Converter{
	Config:         ConfigDefault,
	AddExtractedIp: AddExtractedIpDefault,
}

func (c Converter) AddIpsFromBody(ips []Ip4, body string) []Ip4 {
	return c.AddExtractedIp(ips, body)
}

func (c Converter) MapsToTagged(
	m iter.Seq2[map[string]any, error],
) iter.Seq2[map[string]any, error] {
	return func(yield func(map[string]any, error) bool) {
		tagged := map[string]any{}

		var foundIps []Ip4
		var neoTags []string

		for original, e := range m {
			clear(tagged)
			foundIps = foundIps[:0]
			neoTags = neoTags[:0]

			if nil != e {
				yield(tagged, e)
				return
			}

			for key, val := range original {
				tagged[key] = val
			}

			body, e := c.Config.GetBody(original)
			if nil != e {
				yield(tagged, e)
				return
			}

			var found []Ip4 = c.AddIpsFromBody(foundIps, body)

			tags, e := c.Config.GetTags(neoTags, original)
			if nil != e {
				yield(tagged, e)
				return
			}

			neoTags = tags

			var neo []string = AddToTag(neoTags, found)

			tagged[string(c.Config.TagColumnName)] = neo

			if !yield(tagged, nil) {
				return
			}
		}
	}
}

type Tagged map[string]any
