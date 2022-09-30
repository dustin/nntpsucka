N=${1}
test -z ${1} && N=3
watch "tail -n ${N} *.log"
