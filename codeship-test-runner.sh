#!/bin/bash
#
# This test intends to run each test individually to isolate side effects of tests not shutting down an env properly

set -e

TMP_DIR="/tmp"
START_SPLUNK_LOG="${TMP_DIR}/splunk_start.log";

UTESTS="`find src/test/java/ -iname \*Test\.java | grep -v -i -E 'Abstract|performance_tests|ConnectionSettingsTest'|sed 's#.*/##;s#.java##'`";
ITESTS="`find src/test/java/ -iname \*IT\.java | grep -v -i -E 'Abstract|performance_tests|ConnectionSettingsTest'|sed 's#.*/##;s#.java##'`";

echo "starting test runner";
FAILED_TEST="" ; 
FAIL_CODE="" ;
SHELL_PID=$$; # script process ID

# trap to execute on receiving TERM signal 
trap "exit $FAIL_CODE" TERM;


DOCKER="";

die() {
	echo "dying $1"
	kill -s TERM $SHELL_PID
}

# parse args
while getopts ":d" opt; do
  case $opt in
    d) DOCKER="SET"
    ;;
    \?) echo "Invalid option -$OPTARG" >&2 ; die 1;
    ;;
  esac
done

run_test() {
	echo -n "###`date`###starting $1 ... "; 

	# try running each test file:
	( 
		/usr/bin/time -l mvn test -Dtest=$1  > ${TMP_DIR}/$1.log 2>&1 && 
		echo "succeeded" &&
		grep '^Tests run:' ${TMP_DIR}/$1.log | grep "Time elapsed"
	) || ( # in case a test command above failed:
		FAIL_CODE=$? && 
		echo "failed with exit code: $FAIL_CODE" ;
		FAILED_TEST="$1"; 
		cat ${TMP_DIR}/$1.log ;
		die $FAIL_CODE;
	);
}

start_splunk() {
	echo "starting splunk...";
	(
		/opt/splunk/bin/splunk start > $START_SPLUNK_LOG 2>&1 && 
		echo 'last 100 records from $START_SPLUNK_LOG' && 
		tail -100 $START_SPLUNK_LOG 
	) || (
		FAIL_CODE=$? && 
		tail -10000 $START_SPLUNK_LOG;
		die $FAIL_CODE
	);
}

echo "Run all unit tests one by one"
for f in $UTESTS; do 
	run_test $f || die $? ; 
done ; 


if [ "$DOCKER" ]; then 
	start_splunk
else 
	echo "running locally, expect splunk to be run on the local host for integration tests"
fi

echo "Run all integration tests one by one"
for f in $ITESTS; do 
	run_test $f || die $? ; 
done

echo "Run a performance test"
# run_test ""