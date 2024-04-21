#!/bin/bash
#
# Combine terms to create  n-grams (for n=1,2,3) and then count and sort them
input=$(cat)
#echo "Entire input: $input"
#echo "test bigram"
unigrams=$(echo -e "$input"|sed 's/\t*$//' | sed 's/\s/ /g')
bigrams=$(echo "$input" | awk 'NR>1{print prev, $0} {prev=$0}'| sed 's/\t*$//' | sed 's/\s/ /g' | sort)
trigrams=$(echo "$input" | awk 'NR>2{print prev1, prev2, $0} {prev1=prev2; prev2=$0}' | sed 's/\t*$//' | sed 's/\s/ /g' | sort)

#bigrams=$(echo -e "$("$input" | awk '{for (i=1; i<NF; i++) print $i, $(i+1)}')")
#trigrams=$(echo -e "$(echo "$input" | awk '{for (i=1; i<NF-1; i++) print $i, $(i+1), $(i+2)}')")
ngrams=$(echo -e "$unigrams\n$bigrams\n$trigrams")
ordered_ngrams=$(echo -e "$ngrams" | sort )
echo -e "$ordered_ngrams"

#echo "$ngrams"


#unigrams=$(echo -e "$input"|sed 's/\t*$//' | sed 's/\s/ /g')
#bigrams=$(echo -e "$input"|sed '$!N;s/\n/ /g'|sed 's/\t*$//' | sed 's/\s/ /g')
#trigrams=$(echo -e "$input"|sed '$!N;$!N;s/\n/ /g'|sed 's/\t*$//' | sed 's/\s/ /g')
#ngrams=$(echo -e "$unigrams\n$bigrams\n$trigrams")

#ordered_ngrams=$(echo -e "$ngrams" | sort )



#echo -e "$ordered_ngrams"

#echo -e "$bigrams"

#while IFS= read -r line; do
    # Process each line as needed
    #echo "Input line: $line"
    #echo -e "$1"
    #
    #bigrams=$(echo -e "$line"|sed '$!N;s/\n/ /g'|sed 's/\t*$//' | sed 's/\s/ /g')
    #
    #ngrams=$(echo -e "$unigrams\n$bigrams\n$trigrams")
    #ordered_ngrams=$(echo -e "$ngrams" | sort )
    #echo -e "$ordered_ngrams"
    #echo -e "$bigrams"
#done


