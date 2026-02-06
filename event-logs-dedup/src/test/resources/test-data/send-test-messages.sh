#!/bin/bash

echo '{"index":4,"emittingContractIndex":0,"type":0,"tickNumber":43140712,"epoch":198,"logDigest":"d4d490b96229a3d3","logId":606444,"bodySize":72,"timestamp":1769659921,"transactionHash":"rnhiirdcpffnqcdjlutqdaigfzogtmetzlorawulzcgotmkapufniabfadha","body":{"amount":1124693792,"destination":"SMIAUCMVHWUOLDDRAVAUFYRELZIANSWOLMTDXJTDFCANBUNUOAAZUZECXEKB","source":"BAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAARMID"}}' \
  | kcat -P -b localhost:9092 -t qubic-event-logs-incoming-local

echo '{"index":5,"emittingContractIndex":0,"type":0,"tickNumber":43140712,"epoch":198,"logDigest":"5c9f7b3257579212","logId":606445,"bodySize":72,"timestamp":1769659921,"transactionHash":"wuwapbgqfvtitfrdtdksyefwefueyrrhvhqywrldpgemtbzbiwdgbczajhud","body":{"amount":0,"destination":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFXIB","source":"UNTGFDGPZSUPVAUITIMGVHJMYWVATOKRFTOPRQEYNGTVCSMZWVMJMAODLFYK"}}' \
  | kcat -P -b localhost:9092 -t qubic-event-logs-incoming-local

echo '{"index":6,"emittingContractIndex":0,"type":0,"tickNumber":43140712,"epoch":198,"logDigest":"5c9f7b3257579212","logId":606446,"bodySize":72,"timestamp":1769659921,"transactionHash":"rgrpcekxwoqqxcrzyljmznguysmdwoyvyafetnbbfetivmeiuoxskciepvol","body":{"amount":0,"destination":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFXIB","source":"UNTGFDGPZSUPVAUITIMGVHJMYWVATOKRFTOPRQEYNGTVCSMZWVMJMAODLFYK"}}' \
  | kcat -P -b localhost:9092 -t qubic-event-logs-incoming-local

# subsequent duplicate
#echo '{"index":6,"emittingContractIndex":0,"type":0,"tickNumber":43140712,"epoch":198,"logDigest":"5c9f7b3257579212","logId":606446,"bodySize":72,"timestamp":1769659921,"transactionHash":"rgrpcekxwoqqxcrzyljmznguysmdwoyvyafetnbbfetivmeiuoxskciepvol","body":{"amount":0,"destination":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFXIB","source":"UNTGFDGPZSUPVAUITIMGVHJMYWVATOKRFTOPRQEYNGTVCSMZWVMJMAODLFYK"}}' \
#  | kcat -P -b localhost:9092 -t qubic-event-logs-incoming-local

echo '{"index":7,"emittingContractIndex":0,"type":0,"tickNumber":43140712,"epoch":198,"logDigest":"5c9f7b3257579212","logId":606447,"bodySize":72,"timestamp":1769659921,"transactionHash":"wyukgwvkxtbpladqxcdwqzurrkseydropwxlulbylaokdceeqroouexawkgh","body":{"amount":0,"destination":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFXIB","source":"UNTGFDGPZSUPVAUITIMGVHJMYWVATOKRFTOPRQEYNGTVCSMZWVMJMAODLFYK"}}' \
  | kcat -P -b localhost:9092 -t qubic-event-logs-incoming-local

echo '{"index":0,"emittingContractIndex":0,"type":0,"tickNumber":43140713,"epoch":198,"logDigest":"becd2023180cff2b","logId":606448,"bodySize":72,"timestamp":1769659922,"transactionHash":"ilncbjlizaiavdufghpzkpsztwrbcfuocmfeexpwubzepuixqsjmdnwccqya","body":{"amount":0,"destination":"AFZPUAIYVPNUYGJRQVLUKOPPVLHAZQTGLYAAUUNBXFTVTAMSBKQBLEIEPCVJ","source":"FOODEJFJATCIPBAUCJUNEGSTMPCBPILZRFUTTHPGIFDYCYYHRUCXNICDBCXC"}}' \
  | kcat -P -b localhost:9092 -t qubic-event-logs-incoming-local

echo '{"index":1,"emittingContractIndex":0,"type":0,"tickNumber":43140713,"epoch":198,"logDigest":"acaa15ae9ca4bf32","logId":606449,"bodySize":72,"timestamp":1769659922,"transactionHash":"kjnlvtxuwfismbkfpljvzxbricmamajagcqjqotaxgtlikkiptemisygifii","body":{"amount":0,"destination":"AFZPUAIYVPNUYGJRQVLUKOPPVLHAZQTGLYAAUUNBXFTVTAMSBKQBLEIEPCVJ","source":"KSPTLUCVNIVYIHOHINAJWMNRCTPBBVVZJPBBVMCRIBHQLZTNMJKHFAWCCTSN"}}' \
  | kcat -P -b localhost:9092 -t qubic-event-logs-incoming-local

echo '{"index":2,"emittingContractIndex":0,"type":0,"tickNumber":43140713,"epoch":198,"logDigest":"a9ef38d36f6e0279","logId":606450,"bodySize":72,"timestamp":1769659922,"transactionHash":"tmwngitzwtvxpbbenqayboqotyobmnbfcvxvktfaacnzhlnmybtrvbjewesg","body":{"amount":0,"destination":"AFZPUAIYVPNUYGJRQVLUKOPPVLHAZQTGLYAAUUNBXFTVTAMSBKQBLEIEPCVJ","source":"YQRLXKGYBISZXGAWERHKPZDPNOQBMUHTRVESWCPTFHQFNUAAHHVHAPHFICEH"}}' \
  | kcat -P -b localhost:9092 -t qubic-event-logs-incoming-local

# duplicate
#echo '{"index":1,"emittingContractIndex":0,"type":0,"tickNumber":43140713,"epoch":198,"logDigest":"acaa15ae9ca4bf32","logId":606449,"bodySize":72,"timestamp":1769659922,"transactionHash":"kjnlvtxuwfismbkfpljvzxbricmamajagcqjqotaxgtlikkiptemisygifii","body":{"amount":0,"destination":"AFZPUAIYVPNUYGJRQVLUKOPPVLHAZQTGLYAAUUNBXFTVTAMSBKQBLEIEPCVJ","source":"KSPTLUCVNIVYIHOHINAJWMNRCTPBBVVZJPBBVMCRIBHQLZTNMJKHFAWCCTSN"}}' \
#  | kcat -P -b localhost:9092 -t qubic-event-logs-incoming-local

# late duplicate
#echo '{"index":6,"emittingContractIndex":0,"type":0,"tickNumber":43140712,"epoch":198,"logDigest":"5c9f7b3257579212","logId":606446,"bodySize":72,"timestamp":1769659921,"transactionHash":"rgrpcekxwoqqxcrzyljmznguysmdwoyvyafetnbbfetivmeiuoxskciepvol","body":{"amount":0,"destination":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFXIB","source":"UNTGFDGPZSUPVAUITIMGVHJMYWVATOKRFTOPRQEYNGTVCSMZWVMJMAODLFYK"}}' \
#  | kcat -P -b localhost:9092 -t qubic-event-logs-incoming-local

# invalid duplicate
#echo '{"index":7,"emittingContractIndex":0,"type":0,"tickNumber":43140712,"epoch":198,"logDigest":"5c9f7b3257579212","logId":606446,"bodySize":72,"timestamp":1769659921,"transactionHash":"rgrpcekxwoqqxcrzyljmznguysmdwoyvyafetnbbfetivmeiuoxskciepvol","body":{"amount":0,"destination":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFXIB","source":"UNTGFDGPZSUPVAUITIMGVHJMYWVATOKRFTOPRQEYNGTVCSMZWVMJMAODLFYK"}}' \
#  | kcat -P -b localhost:9092 -t qubic-event-logs-incoming-local