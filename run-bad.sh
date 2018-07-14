runId=1
i=1
numberPararelism=-1
tn=0
rn=0
current_tn=0
current_rn=0
pids=()
current_ep=False
badNodesS=0
badNodesE=0
increment=5


if [ -z "$1" ]
then
  echo "Include number of times to run cycle"
  exit -1
fi
if [ -z "$3" ]
then
  echo "Input filename"
  exit -1
fi
if ! [ -z "$2" ]
then
  numberPararelism=$2
fi
if ! [ -z "$6" ]
then
  current_ep=$6
fi

if ! [ -z "$7" ]
then
  badNodesS=$7
  badNodesE=$8
fi

cycles=$1
filename=$3
tn=$4
rn=$5

#sh ./run-single-n.sh $cycles $para $filename 0 0 $ep $bn
#sh ./run-single-n.sh $cycles $para $filename $tn $rn $ep $bn
((aCycles=$1*(badNodesE/increment)))


while [ "$i" -le "$aCycles" ]
do

    echo run: $runId -ln $filename -tn $current_tn -rn $current_rn -ep $current_ep -bn $badNodesS

        if [ "$numberPararelism" -eq "-1" ]
    then
      if [ "$runId" -eq "1" ]
      then
        pypy echo.py conf_echo/ $runId -sn True
      else
        pypy echo.py conf_echo/ $runId -ln $filename -tn $current_tn -rn $current_rn -ep $current_ep -bn $badNodesS
      fi
    else
      if [ "$runId" -eq "1" ]
      then
        pypy echo.py conf_echo/ $runId -sn True &
        pids[$runId]=$!
        sleep 10 
      else
        pypy echo.py conf_echo/ $runId -ln $filename -tn $current_tn -rn $current_rn -ep $current_ep -bn $badNodesS & 
        pids[$runId]=$!
      fi
      ((numberPararelism=numberPararelism-1))
    fi


    if [ $(( $runId % $cycles)) -eq 0 ]; 
    then
    	(( badNodesS=badNodesS+increment ))
	fi

    if [ "$numberPararelism" -eq "0" ] 
    then
      for pid in ${pids[*]}; do
        wait $pid
        ((numberPararelism=numberPararelism+1))
      done
      pids=()
    fi

    (( runId=runId+1 ))
    (( i=i+1 ))
done