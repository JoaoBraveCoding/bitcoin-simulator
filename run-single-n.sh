# To be able to run this script run:
# chmod 755 run-single-n.sh

runId=1
i=1
numberPararelism=-1
tn=0
rn=0
pids=()
current_ep=False
badNodes=0
behaviour=0


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
  badNodes=$7
  behaviour=$8
fi

cycles=$1
filename=$3
tn=$4
rn=$5


while [ "$i" -le "$cycles" ]
do

    echo run: $runId -ln $filename -tn $tn -rn $rn -ep $current_ep -bm $badNodes -bh $behaviour
    if [ "$numberPararelism" -eq "-1" ]
    then
      if [ "$runId" -eq "1" ]
      then
        pypy echo.py conf_echo/ $runId -sn True
      else
        pypy echo.py conf_echo/ $runId -ln $filename -tn $tn -rn $rn -ep $current_ep -bm $badNodes $behaviour
      fi
    else
      if [ "$runId" -eq "1" ]
      then
        pypy echo.py conf_echo/ $runId -sn True &
        pids[$runId]=$!
        sleep 10 
      else
        pypy echo.py conf_echo/ $runId -ln $filename -tn $tn -rn $rn -ep $current_ep -bm $badNodes $behaviour & 
        pids[$runId]=$!
      fi
      ((numberPararelism=numberPararelism-1))
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
