#!/usr/bin/env bash
while `true`
do 
   CMD=/home/cdd/feedparser/t1.py
   date
   echo Running $CMD
   $CMD
   echo sleeping for 4 hours
   sleep 4h
done


