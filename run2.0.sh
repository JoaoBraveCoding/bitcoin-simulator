# To be able to run this script run:
# chmod 755 run.sh
# run
runId=1
i=0
numberPararelism=-1
tn=0
rn=0
pids=()
ep=False
current_ep=False
badNodes=0
behaviour=0


if [ -z "$1" ]
then
  echo "Include number of times to run cycle"
  exit -1
fi
if [ -z "$2" ]
then
  echo "Input filename"
  exit -1
fi
if ! [ -z "$3" ]
then
  numberPararelism=$3
fi
if ! [ -z "$4" ]
then
  ep=$4
fi

if ! [ -z "$5" ]
then
  badNodes=$5
  behaviour=$6
fi


if [ "$ep" = "False" ]
then
  (( cycles=$1*4-1))
else
  (( cycles=$1*4*2-1))
fi

filename=$2


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

    if [ "$rn" -eq "1" ]
    then
      ((tn=tn-1))
      ((rn=tn))
    else
      ((rn=1))
    fi

    if [ "$tn" -eq "-1" ] || [ "$rn" -eq "1" ] && [ "$tn" -eq "0" ]
    then
      if [ "$ep" = "True" ]
      then  
        if  [ "$current_ep" = "False" ]
        then
          current_ep=True
        else
          current_ep=False
        fi
      fi
      ((tn=2))
      ((rn=2))
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
