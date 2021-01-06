#!/bin/bash
# Test script to test hpfs filesystem operations.
# Usage: ./test.sh

fsdir=./testrun
mntdir=./testrun/mnt
rwdir=./testrun/mnt/rw
rodir=./testrun/mnt/ro
hpfs=../build/hpfs
trace=none

rm -r ./testrun > /dev/null 2>&1
mkdir -p $fsdir/seed > /dev/null 2>&1

echo "Create a seed text file with random text."
tr -dc A-Za-z0-9 </dev/urandom | head -c 10240 > $fsdir/seed/sample.txt

echo "Start hpfs process with merge support."
./$hpfs fs $fsdir $mntdir merge=true trace=$trace &
pid=$!
sleep 1

echo "Start RW session with hash map enabled."
touch $mntdir/::hpfs.rw.hmap

echo "Start RO session 1."
touch $mntdir/::hpfs.ro.hmap.ro

echo "Perform some filesystem operations on the RW session."
mkdir $rwdir/dir1
mkdir $rwdir/dir2
mv $rwdir/dir2 $rwdir/dir2_renamed
cp $rwdir/sample.txt $rwdir/dir2_renamed/copied.txt
truncate -s 100K $rwdir/dir2_renamed/copied.txt
rmdir $rwdir/dir1
rm $rwdir/sample.txt

echo "Read from RO session before RW session is ended."
echo "RO session 1: Read from sample.txt"
head -c 10 $rodir/sample.txt
echo ""

echo "Stop RW session."
rm $mntdir/::hpfs.rw.hmap

echo "Read from RO session after RW session is ended."
echo "RO session 1: Read from sample.txt"
head -c 10 $rodir/sample.txt
echo ""

echo "Stop RO session 1."
rm $mntdir/::hpfs.ro.hmap.ro

echo "Start RO session 2."
touch $mntdir/::hpfs.ro.hmap.ro

echo "Read from RO session 2."
echo "RO session 2: Check for now-deleted sample.txt"
stat $rodir/sample.txt

echo "Stop RO session 2."
rm $mntdir/::hpfs.ro.hmap.ro

sleep 1
kill $pid
sleep 1

echo "Verify whether operations are merged to seed."
echo "Reading seed/dir2_renamed/copied.txt"
head -c 10 $fsdir/seed/dir2_renamed/copied.txt
echo ""

# Clean up test directory.
rm -r ./testrun > /dev/null 2>&1