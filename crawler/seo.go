package crawler

import (
	"bytes"
	"strings"

	"golang.org/x/net/html"
)

// extractSEO parses an HTML document and reports its on-page SEO signals:
// the <title> text, the content of <meta name="description">, and whether
// an <h1> is present. Only the first occurrence of each tag counts; later
// duplicates are ignored. html.Tokenizer decodes entities for both text
// content (Text()) and attribute values (as part of Token()), so values
// like "Fish &amp; Chips" come out already as "Fish & Chips".
func extractSEO(body []byte) SEO {
	var seo SEO
	tokenizer := html.NewTokenizer(bytes.NewReader(body))

	for {
		tt := tokenizer.Next()
		if tt == html.ErrorToken {
			return seo
		}
		if tt != html.StartTagToken && tt != html.SelfClosingTagToken {
			continue
		}

		token := tokenizer.Token()
		switch token.Data {
		case "title":
			captureTitle(tokenizer, tt, &seo)
		case "meta":
			captureMetaDescription(token, &seo)
		case "h1":
			seo.HasH1 = true
		}
	}
}

// captureTitle records the first <title> tag's text, if any is present as
// the immediately following text token. It is a no-op once a title has
// already been captured, so later duplicates are ignored.
func captureTitle(tokenizer *html.Tokenizer, tt html.TokenType, seo *SEO) {
	if seo.HasTitle {
		return
	}
	seo.HasTitle = true
	if tt == html.StartTagToken && tokenizer.Next() == html.TextToken {
		seo.Title = strings.TrimSpace(string(tokenizer.Text()))
	}
}

// captureMetaDescription records the content of the first
// <meta name="description" content="..."> tag. It is a no-op once a
// description has already been captured, so later duplicates are ignored.
func captureMetaDescription(token html.Token, seo *SEO) {
	if seo.HasDescription {
		return
	}
	var name, content string
	for _, attr := range token.Attr {
		switch strings.ToLower(attr.Key) {
		case "name":
			name = strings.ToLower(strings.TrimSpace(attr.Val))
		case "content":
			content = attr.Val
		}
	}
	if name == "description" {
		seo.HasDescription = true
		seo.Description = strings.TrimSpace(content)
	}
}
