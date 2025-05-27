iperf TCP
coordinator -> other nodes // (same client to coordinaator)
[  5]   0.00-10.00  sec   237 MBytes   198-199 Mbits/sec   24             sender
[  5]   0.00-10.00  sec   233 MBytes   196 Mbits/sec                  receiver


or  
[ ID] Interval           Transfer     Bitrate         Retr
[  5]   0.00-10.00  sec   237 MBytes  23.7 MBytes/sec   15             sender
[  5]   0.00-10.00  sec   234 MBytes  23.3 MBytes/sec                  receiver

1mb = 43 ms

sudo hdparm -Tt /dev/sda
local
 Timing cached reads:   43376 MB in  1.99 seconds = 21746.63 MB/sec
 Timing buffered disk reads: 9518 MB in  3.00 seconds = 3172.22 MB/sec

lab
 Timing cached reads:   16622 MB in  1.99 seconds = 8362.37 MB/sec = 1mb => 0.12 ms
 Timing buffered disk reads: 164 MB in  3.03 seconds =  54.17 MB/sec = 1mb => 18.4 ms



local 3.04 MiB/s = 3040 KiB/s
lab 615.1 KiB/s => 

398 kB/sec

ubuntu@ubuntu:~$ fio --name=randread --ioengine=sync --iodepth=1 --rw=randread --bs=4k --numjobs=1 --size=10G --runtime=30m --time_based --direct=1
randread: (g=0): rw=randread, bs=(R) 4096B-4096B, (W) 4096B-4096B, (T) 4096B-4096B, ioengine=sync, iodepth=1
fio-3.36
Starting 1 process
randread: Laying out IO file (1 file / 10240MiB)
Jobs: 1 (f=1): [r(1)][100.0%][r=384KiB/s][r=96 IOPS][eta 00m:00s]
randread: (groupid=0, jobs=1): err= 0: pid=66685: Fri Apr  4 00:48:06 2025
  read: IOPS=97, BW=389KiB/s (398kB/s)(683MiB/1800009msec)
    clat (usec): min=280, max=256877, avg=10283.58, stdev=6171.23
     lat (usec): min=280, max=256877, avg=10284.12, stdev=6171.23
    clat percentiles (msec):
     |  1.00th=[    3],  5.00th=[    4], 10.00th=[    4], 20.00th=[    6],
     | 30.00th=[    8], 40.00th=[    9], 50.00th=[   10], 60.00th=[   11],
     | 70.00th=[   13], 80.00th=[   15], 90.00th=[   17], 95.00th=[   18],
     | 99.00th=[   27], 99.50th=[   32], 99.90th=[   82], 99.95th=[  103],
     | 99.99th=[  142]
   bw (  KiB/s): min=   96, max=  505, per=99.82%, avg=388.78, stdev=41.70, samples=3599
   iops        : min=   24, max=  126, avg=97.20, stdev=10.43, samples=3599
  lat (usec)   : 500=0.07%, 750=0.03%, 1000=0.01%
  lat (msec)   : 2=0.42%, 4=9.87%, 10=40.57%, 20=47.10%, 50=1.74%
  lat (msec)   : 100=0.14%, 250=0.05%, 500=0.01%
  cpu          : usr=0.15%, sys=0.45%, ctx=174920, majf=0, minf=12
  IO depths    : 1=100.0%, 2=0.0%, 4=0.0%, 8=0.0%, 16=0.0%, 32=0.0%, >=64=0.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     issued rwts: total=174918,0,0,0 short=0,0,0,0 dropped=0,0,0,0
     latency   : target=0, window=0, percentile=100.00%, depth=1

Run status group 0 (all jobs):
   READ: bw=389KiB/s (398kB/s), 389KiB/s-389KiB/s (398kB/s-398kB/s), io=683MiB (716MB), run=1800009-1800009msec

Disk stats (read/write):
  vda: ios=174906/917, sectors=1399248/13912, merge=0/804, ticks=1792453/29768, in_queue=1849392, util=99.35%