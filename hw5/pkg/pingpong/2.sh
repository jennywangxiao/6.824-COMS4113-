for t in TestHashAndEqual TestStateInherit TestNextStates TestPartition TestBasic TestBfsFind TestBfsFindAll1 TestBfsFindAll2 TestBfsFindAll3 TestRandomWalkFindAll TestRandomWalkFind 
do
  echo $t
  count=0
  n=20
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