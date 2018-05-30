# To be able to run this script run:
# chmod 755 run.sh
# run
runId=1
cycles=0
tpNodes=0
i=0
filename=250-2-0

if [ -z "$1" ]
then
  echo "Include number of times to run cycle"
  exit -1
fi


cycles=$1
((cycles=cycles*3-1))
while [ "$i" -le "$cycles" ]
do
  if [ "$tpNodes" -eq "0" ]
  then
    if [ "$i" -eq "0" ]
    then
      if [ -z "$2" ]
      then
        pypy echo.py conf_echo/ 1 -sn True -tn $tpNodes
      else
        pypy echo.py conf_echo/ $runId -ln $filename -tn $tpNodes
      fi
    else
      pypy echo.py conf_echo/ $runId -ln $filename -tn $tpNodes
    fi
  else
    pypy echo.py conf_echo/ $runId -ln $filename -tn $tpNodes
  fi

  (( runId=runId+1 ))
  (( i=i+1))
  (( tpNodes=tpNodes-1))
  if [ "$tpNodes" -eq "-1" ]
  then
    ((tpNodes=2))
  fi
done
