#!/usr/bin/env sh

URLS=$(curl "https://aws.amazon.com/api/dirs/items/search?item.directoryId=amazon-redwood&sort_by=item.additionalFields.sortDate&sort_order=desc&size=24&item.locale=en_US&tags.id=GLOBAL%23content-type%23article" |
    jq '.items[].item.additionalFields.headlineUrl')

printf %s "$URLS" |
    while IFS= read -r URL; do
        echo ""
        echo "[[websites]]"
        echo "url = $URL"
    done
