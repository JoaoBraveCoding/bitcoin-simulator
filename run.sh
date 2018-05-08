# To be able to run this script run:
# chmod 755 run.sh
# run 1 50 500
i=50
runId=1
nbNodes=0
cycleInc=1

if [ -z "$1" ]
then
  echo "ERROR: please input at least the max number of nodes"
  exit -1
fi

if [ -z "$3" ]
then
  echo "I got $1"
  nbNodes=$1
elif [ -z "$4" ];
then
  echo "I got $1 and $2 and $3"
  runId=$1
  i=$2
  nbNodes=$3
else
  echo "I got $1 and $2 and $3 and $4"
  runId=$1
  cycleInc=$2
  i=$3
  nbNodes=$4
fi


while [ "$i" -le "$nbNodes" ]
do
  echo "This is runID $runId with this nodes $i"
  pypy echo.py conf_echo/ $runId $i
  (( runId=runId+cycleInc ))
  (( i=i+50*cycleInc))
done
