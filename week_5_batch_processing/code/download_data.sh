
set -e

# https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2020-01.csv.gz


TAXI_TYPE=$1  #"fhvhv"
YEAR=$2 # 2021

URL_PREFIX="https://github.com/DataTalksClub/nyc-tlc-data/releases/download"


# for MONTH in {1..12}; do
for MONTH in {6}; do
    FMONTH=`printf "%02d" ${MONTH}`

    URL="${URL_PREFIX}/${TAXI_TYPE}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv.gz"

    echo "downloading ${URL} to ${LOCAL_PATH}"
    LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
    LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.csv.gz"
    LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

    mkdir -p ${LOCAL_PREFIX}

    wget ${URL} -O ${LOCAL_PATH}
done