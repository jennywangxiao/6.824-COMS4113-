for t in TestCase5Failures
do
  echo $t
  count=0
  n=20
  for i in $(seq 1 $n)
  do
    go test -run "^${t}$" -timeout 2m > ./2-${t}-${i}.txt
    result=$(grep -E '^PASS$' 2-${t}-${i}.txt| wc -l)
    count=$((count + result))
    if [ $result -eq 1 ]; then
       rm ./2-${t}-${i}.txt
    fi
  done
  echo "$count/$n"
done