# To be able to run this script run:
# chmod 755 run.sh
# run
runId=1
i=0
numberPararelism=-1
tn=0
rn=0
pids=()


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
(( cycles=$1*6-1))
filename=$2


#pypy echo.py conf_echo/ 1 -sn True
while [ "$i" -le "$cycles" ]
do
    echo run: $runId -ln $filename -tn $tn -rn $rn
    if [ "$numberPararelism" -eq "-1" ]
    then
      #pypy echo.py conf_echo/ $runId -ln $filename -tn 2
      if [ "$runId" -eq "1" ]
      then
        pypy echo.py conf_echo/ $runId -sn True
      else
        pypy echo.py conf_echo/ $runId -ln $filename -tn $tn -rn $rn
      fi
    else
      if [ "$runId" -eq "1" ]
      then
        pypy echo.py conf_echo/ $runId -sn True &
        pids[$runId]=$!
        sleep 10 
      else
        pypy echo.py conf_echo/ $runId -ln $filename -tn $tn -rn $rn &
        pids[$runId]=$!
      fi
      ((numberPararelism=numberPararelism-1))
    fi


    if [ "$rn" -eq "0" ]
    then
      ((tn=tn-1))
      ((rn=tn))
    else
      ((rn=rn-1))
    fi

    if [ "$tn" -eq "-1" ]
    then
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
