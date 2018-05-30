# To be able to run this script run:
# chmod 755 run.sh
# run
runId=1
i=0

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



cycles=$1
filename=$2
pypy echo.py conf_echo/ 1 -sn True
(( runId=runId+1 ))
while [ "$i" -le "$cycles" ]
do
    pypy echo.py conf_echo/ $runId -ln $filename -tn 2
    (( runId=runId+1 )) 
    pypy echo.py conf_echo/ $runId -ln $filename -tn 2 -rn 0
    (( runId=runId+1 ))
    pypy echo.py conf_echo/ $runId -ln $filename -tn 2 -rn 1
    (( runId=runId+1 ))
    pypy echo.py conf_echo/ $runId -ln $filename -tn 1
    (( runId=runId+1 ))
    pypy echo.py conf_echo/ $runId -ln $filename -tn 1 -rn 0
    (( runId=runId+1 ))
    pypy echo.py conf_echo/ $runId -ln $filename -tn 0
    (( runId=runId+1 ))
    (( i=i+1 ))
done
