## graphing the top shareholders

using the **nasdaq.com** API which itself aggregates the
**sec.gov** filings API.

Some latest findings can be visited at
[defgsus.github.io/billion-bubbles/](https://defgsus.github.io/billion-bubbles/)

The german *billion* is actually equal to the english *trillion*.
That's the range in which the top-top companies operate. So
this repo could as well be called *trillion troubles*
instead of *billion bubbles*. The bubbles, though, are 
picked as the means of representation of companies 
and shareholders.


### development

Run the typical *python env and pip requirements* stuff then

for example:

```bash
python bubble.py --company MSFT \
    --depth 23 --min-share-value 10_000_000 \
    --output graph.gml
```

... to start at **Microsoft** and follow all shareholders and insiders
and the respective companies connected to them, up to a 
branching level of **23**, while ignoring all shareholders
below a position of **10 million** dollars market value.
Finally render everything into a portable graph format.

This will run, unfortunately, several days, and the nasdaq.com
database is stressed a lot. In fact, querying the complete
list of company holders or holder positions can lead to request 
timeouts of 40 seconds, even though the page sizes 
are relatively small. Requests are repeated 3 times
until they eventually work or the whole scraper fails,
which did not happen yet.

The sqlite file is growing a lot. Let's say after visiting
5000 companies and their connected entries it's about 3.5Gb.
It probably can save a lot of space when ignoring the
stock charts, but i deem them to be quite useful at some point. 

