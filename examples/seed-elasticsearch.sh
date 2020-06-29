#!/usr/bin/env bash

texts[0]="First"
texts[1]="Second"
texts[2]="Third"
texts[3]="Fourth"
texts[4]="Fifth"
texts_size=${#texts[@]}

statuses[0]="Active"
statuses[1]="Inactive"
statuses_size=${#statuses[@]}

bools[0]="true"
bools[1]="false"
bools_size=${#bools[@]}

maybes[0]="null"
maybes[1]='"not-null"'
maybes_size=${#maybes[@]}

for i in {1..100}
do
    curl \
        -H "content-type: application/json" \
        -XPUT \
        -d "{\
            \"id\":\"$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1)\",\
            \"status\":\"${statuses[$(($RANDOM % $statuses_size))]}\",\
            \"text_field\":\"${texts[$(($RANDOM % $texts_size))]}\",\
            \"range_field\":$(expr 1000 '*' $(shuf -i 10-99 -n 1)),\
            \"bool_field\":${bools[$(($RANDOM % $bools_size))]},\
            \"maybe_field\":${maybes[$(($RANDOM % $maybes_size))]}\
        }" "http://localhost:9200/the-idx/_doc/${i}"
done