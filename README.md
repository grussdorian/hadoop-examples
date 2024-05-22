# Hadoop Exercise


* Install docker in the machine
* In the toplevel directory with the Dockefile, run 
* `docker image build -t se_assignment:2 .`   
the above command will build the image, download all the data needed for the assignment, run ant build command and then runAllExamples.
* ``cd .. && docker run --user 1000:1000 --rm -it -v `realpath ./se2-w-2023-assignment2`:/Source se_assignment:2  bash`` will run a docker container.
* In the resultant shell, navigate to the source directory, `cd /Source`, where the following commands can be run.   
* To initialize the repository, execute `ant init` command.


### The tasks are described below

## Task #1 Page view frequency ##

### Problem Description ###
There is an apache log showing links that have been accessed by clients.
The task is to create a MapReduce program that counts the total number of times a given url has been accessed.
If the url we get does not start with a valid hostname, we should prepend it with *http://localhost/* (see general notes).

**Expected Output:** URL → frequency

```
#!csv

http://localhost/tikiwiki-2.1/css/admin.css 7
http://localhost/tikiwiki-2.1/tiki-admin.php 308
…
```

### Task #2 Frequency of NYC taxi rides within a 1 hour window ###
###Task Description###
There is a csv data that describes taxi services in the city of Newyork.   
For each hourly period, or hour, of the day, determine the number of taxis in operation during the period.  
The hourly period is defined as the period between the start of an hour and the last second of that hour e.g. from 12.00.00 to 12.59.59.   
Some taxi services can span more than one hour period. In that case, consider the operation in each different time period as unique/different operation.  

**Expected Output:** timeslot/window → frequency

```
#!csv

1am 301935
1pm 548485
10am 504387
…
…
```

