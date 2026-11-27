module github.com/nostalume/mountweave/browser

go 1.27.0

require (
	github.com/chromedp/cdproto v0.157.8
	github.com/chromedp/chromedp v0.20.0
	github.com/nostalume/mountweave v0.4.0
)

require github.com/go-json-experiment/json v0.0.0-20260213210345-44df1a37e875 // indirect

replace github.com/nostalume/mountweave => ..
