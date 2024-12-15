### 1, run 
```sh
crontab -e
```
### 2, scroll down and add
```crontab
0 14-18/2 * * * source ~/anaconda3/etc/profile.d/conda.sh && conda activate dataops && cd ~/Kewwi && i>
MAILTO="binbill472003@gmail.com"
```
