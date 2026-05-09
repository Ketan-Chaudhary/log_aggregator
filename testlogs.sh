#!/bin/bash

LOG_FILE="app.log"

for i in {1..1000}
do
  echo "{\"level\":\"INFO\",\"msg\":\"user login\",\"request_id\":\"$i\",\"timestamp\":\"$(date -Iseconds)\"}" >> $LOG_FILE
done
