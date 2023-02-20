# DS-hw0

Google Drive: https://drive.google.com/drive/folders/1609m49TbsVMcBiiyjXvLL-2UYCCwQimo

Step four: wordcount.py

Step five: bigrams.py / bigram_frequency.py

Command to Run: 

spark-submit ./bigrams.py ./wiki.txt ./outputBigrams


Outputs will be found in 'outputs.zip' which contains outputWordCount (step4), outputBigramCount, and outputBigramFreq 

Output bigram_frequency Ex)

(('language', 'tucker'), 0.00014275517487508923) --> (first word, second word), (probability of a word based only on its previous word)

Output bigram ex)

(('is', 'the'), 13323) --> (first word, second word), (count of number of times instance occured)


