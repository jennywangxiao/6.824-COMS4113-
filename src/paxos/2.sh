for t in TestBasic TestDeaf TestForget TestManyForget TestForgetMem TestRPCCount TestMany TestOld TestManyUnreliable TestPartition 
do
  echo $t
  count=0
  n=5
  for i in $(seq 1 $n)
  do
    go test -run "^${t}$" -timeout 2m > ./0-${t}-${i}.txt
    result=$(grep -E '^PASS$' 0-${t}-${i}.txt| wc -l)
    count=$((count + result))
    if [ $result -eq 1 ]; then
       rm ./0-${t}-${i}.txt
    fi
  done
  echo "$count/$n"
done