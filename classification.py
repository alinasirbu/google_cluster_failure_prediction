#This file contains commands required to train RF ensembles and evaluate 
#on the last 14 days of the google trace. This file is distributed together with
#file big_query.sh  
#
#Copyright (C) 2015  Alina Sirbu, alina.sirbu@unibo.it
#
#This program is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
#To use it directly run in the same directory where the files safe24.csv.gz and fail24.csv.gz are
#(those files are obtained after running all commands in file big_query.sh)


import csv
import numpy as np
import matplotlib.mlab as ml
from sklearn import svm
from sklearn.metrics import roc_curve, auc
import gzip 
import pickle as pkl
import time
import pylab as pl
from sklearn import naive_bayes as nb
from sklearn import neighbors as nn
from sklearn import ensemble as en
from sklearn.metrics import roc_auc_score
from sklearn.metrics import average_precision_score
from sklearn.metrics import precision_recall_curve
import math
import gc


#sort  data and remove headers
files=['safe24']
for f in files:
	print('reading file ',f+'.csv.gz')
	r=gzip.open(f+'.csv.gz','r')
	d=list(csv.reader(r,delimiter=','))
	d[1:]=[x for x in d if x[0]!='f0_']
	d[1:]=sorted(d[1:], key=lambda x:float(x[1]))
	print(len(d))
	r.close()
	r=gzip.open(f+'.csv.gz','w')
	w=csv.writer(r,delimiter=',')
	print('writing..')
	w.writerows(d)
	r.close()


##function that tests the classifier 'clf' on the 'data' with labels 'y' and saves accuracy,
##sensitivity, specificity, precision, F and Mathews coefficient in the file 'filename'

def testing(filename, data,y, clf): #for 2 classes only
	o=clf.predict(data)
	TP=long(sum([o[i]==1 and  y[i]==1 for i in range(len(y)) ]))
	TN=long(sum([o[i]==0 and y[i]==0 for i in range(len(y)) ]))
	FP=long(sum([o[i]==1 and y[i]==0 for i in range(len(y)) ]))
	FN=long(sum([o[i]==0 and y[i]==1 for i in range(len(y)) ]))
	c2_acc=(TP+TN)/np.float(TP+TN+FP+FN)
	if TP+FN>0:
		c2_sens=TP/float(TP+FN)
	else:
		c2_sens=np.nan
	if FP+TN>0:
		c2_spec=TN/float(FP+TN)
	else:
		c2_spec=np.nan
	if TP+FP>0:
		c2_prec=TP/float(TP+FP)
	else:
		c2_prec=np.nan
	F=2*TP/float(2*TP+FP+FN)
	if math.sqrt(TP+FP)*math.sqrt(TP+FN)*math.sqrt(TN+FP)*math.sqrt(TN+FN)>0:
		M=(TP*TN-FP*FN)/float(math.sqrt(TP+FP)*math.sqrt(TP+FN)*math.sqrt(TN+FP)*math.sqrt(TN+FN))
	else:
		M=np.nan
	print(filename,c2_acc,c2_sens,c2_spec,c2_prec,F,M)
	pkl.dump((c2_acc,c2_sens,c2_spec,c2_prec,F,M,o,y), file=gzip.open(filename+'.pkl.gz','wb'))




#replace missing values with 0
def my_float(s):
	if s=='':
		return 0
	return float(s)


#load data
files=['safe24','fail24']
alld=[0,0]
loc=1
for f in files:
	print('reading file ',f+'.csv.gz')
	d=[]
	f=csv.reader(gzip.open(f+'.csv.gz','r'),delimiter=',')
	header=f.next()
	for row in f:
		d.append([my_float(x) for x in row])
	alld[loc]=d
	loc=0
	gc.collect()



##train an ensemble of Random Forests for each benchmark (test day) 
nsafe=[0.25, 0.5, 1,2,3,4]#size of the 'safe' data in the training dataset (as fraction of size of 'fail' data)
train_days=10 #number of days to use for training (defining the benchmarks)
test_days=1 #number of days to use for testing (defining the benchmarks)
step=1 #number of days between benchmarks
tree_count=[2,3,4,5,6,7,8,9,10,11,12,13,14,15]  #number of trees in the Random Forest
runs=5 #number of Random forests to be built for each (nsafe,tree_count) combination
set=24  #threshold for class definition - for logging purposes only
cthreshold=0 #threshold for correlation between features and time to next remove - 0 means all fetaures are used

###extract features with corr>cthreshold
corrs=list(csv.reader(open('feature_time_corr_real.csv','r'))) #this file contains correlation between each feature and the time to remove
fs=[corrs[0][i] for i in range(len(corrs[1])) if float(corrs[1][i])**2>=(cthreshold**2)]
cols=[i for i in range(4,len(d[0])) if header[i][3:] in fs ]
cols.append(len(d[0])-1)


for i in range(2,29-train_days-test_days,step): #i is first day used for training skip first 2 days
	print('forward validation  run: ',i)	#every pass through this for loop is a benchmark
	#first create test and train data
	traind=[]
	testd=[[],[]]
	start_train=600000000+i*24*3600000000
	end_train=start_train+train_days*24*3600000000
	start_test=end_train+set*3600000000
	end_test=start_test+24*test_days*3600000000
	
	#test data is the same for all RFs for this benchmark
	for j in range(2):
		rd=[x for x in alld[j] if x[1]>start_train and x[1]<end_train]
		traind.append(rd) #train data - fail and safe class are separate
		rd=[x for x in alld[j] if x[1]>start_test and x[1]<end_test]
		testd[0].extend(rd[:(len(rd)//2)]) #individual test data
		testd[1].extend(rd[(len(rd)//2):])#ensemble test data
	
	testx=[0,0]
	testy=[0,0]#two testing stages
	testTime=[0,0]
	testMachine=[0,0]
	testTTR=[0,0]
	for t in range(2):
		print('setting up train and test',t)
		testd[t]=np.array(testd[t])
		testx[t]=testd[t][:,cols]
		testy[t]=testd[t][:,0]
		testTime[t]=testd[t][:,1]
		testMachine[t]=testd[t][:,2]
		testTTR[t]=testd[t][:,3]
		traind[t]=np.array(traind[t])
	
	
	#test data is ready, save it to file for future use
	pkl.dump((testTime, testMachine,testTTR),gzip.open(str(set)+'testData'+'_forward'+str(train_days)+'d'+str(test_days)+'dxval'+str(i)+'_corr'+str(cthreshold)+'.pkl.gz','w'))
	start=0 #index in the safe training data
	for l in range(runs):	#repeat runs times
		for tc in tree_count: #repeat for each tree count
			print('tree count: ',tc)
			for ns in nsafe: #repeat for each nsafe
				#first reshuffle if necessary
				end=start+int(ns*len(traind[0]))
				if end>len(traind[1]):
					start=0
					end=start+int(ns*len(traind[0]))
				if start==0:
					print('shuffling safe train data')
					np.random.shuffle(traind[1])
				#obtain training dataset by combining all fail data points with a subset of the safe data points
				trainx=np.vstack((traind[0],traind[1][start:end,:]))
				
				trainTTR=trainx[:,3]
				trainy=trainx[:,0] #training labels
				trainx=trainx[:,cols] #training features
				
				#create classifier
				clf =en.RandomForestClassifier(n_estimators=tc)
				#train
				clf.fit(trainx, trainy)
				print('testing..')
				#test on training data
				testing(str(set)+'trainRF'+str(tc)+'_forward'+str(train_days)+'d'+str(test_days)+'dxval'+str(i)+'_corr'+str(cthreshold)+'_nsafe'+str(ns)+'run'+str(l),trainx,trainy,clf)
				#test on individual test data
				testing(str(set)+'test0RF'+str(tc)+'_forward'+str(train_days)+'d'+str(test_days)+'dxval'+str(i)+'_corr'+str(cthreshold)+'_nsafe'+str(ns)+'run'+str(l),testx[0],testy[0],clf)
				#test on ensemble test data
				testing(str(set)+'test1RF'+str(tc)+'_forward'+str(train_days)+'d'+str(test_days)+'dxval'+str(i)+'_corr'+str(cthreshold)+'_nsafe'+str(ns)+'run'+str(l),testx[1],testy[1],clf)
				start=end
				


#compute aupr and auroc after combining all RFs for each benchmark - including ROC/PR plots
f=open(str(set)+'_forward'+str(train_days)+'d'+str(test_days)+'_auroc_prec_eps.txt','w')
epsilon=0.001
for r in range(2,29-train_days-test_days,step): #i is first day used for training
	print('xval',r)
	os=[]
	precisions=[]
	recalls=[]
	FPRs=[]
	for ns in nsafe:
		pr=[]
		rc=[]
		FPR=[]
		print('nsafe',ns)
		for l in range(runs):
			for tc in tree_count:
				#load next classifier results on individual test data
				c2_acc,c2_sens,c2_spec,c2_prec,F,M,o,y=pkl.load(gzip.open(str(set)+'test0RF'+str(tc)+'_forward'+str(train_days)+'d'+str(test_days)+'dxval'+str(r)+'_corr'+str(cthreshold)+'_nsafe'+str(ns)+'run'+str(l)+'.pkl.gz','rb'))
				if not math.isnan(c2_prec):
					#compute weight for this classifier (p)
					p=c2_prec+epsilon
					#load classifier results on ensemble test data
					c2_acc,c2_sens,c2_spec,c2_prec,F,M,o,y=pkl.load(gzip.open(str(set)+'test1RF'+str(tc)+'_forward'+str(train_days)+'d'+str(test_days)+'dxval'+str(r)+'_corr'+str(cthreshold)+'_nsafe'+str(ns)+'run'+str(l)+'.pkl.gz','rb'))
					#weighted vote on ensemble test data
					os.append([x*p for x in o])
					pr.append(c2_prec)
					rc.append(c2_sens)
					FPR.append(1-c2_spec)
		precisions.append(pr)
		recalls.append(rc)
		FPRs.append(FPR)
	if len(os)>0:
		#sum the votes
		o=[sum([x[j] for x in os]) for j in range(len(os[0]))]
		#normalise to [0,1]
		m=float(max(o))
		o=[x/m for x in o]
		print(max(o))
		#compute auroc and aupr and save to file
		auroc=roc_auc_score(y,o)
		aupr=average_precision_score(y,o)
		f.write(str((r,auroc,aupr))+'\n')
		#plot ROC and PR
		roc=roc_curve(y,o)
		prc=precision_recall_curve(y,o)
		pl.close()
		pl.figure(figsize=(10,4.4))
		pl.subplot(121)
		for i in range(len(FPRs)):
			pl.plot(FPRs[i],recalls[i],'.')
		pl.plot(roc[0],roc[1],linewidth=2)
		pl.plot((0,1),(0,1),':',color='grey')
		pl.plot((0.1,0.1),(0,1),'--',color='grey')
		pl.plot((0.05,0.05),(0,1),'--',color='grey')
		pl.plot((0.01,0.01),(0,1),'--',color='grey')
		pl.xlabel('False Positive Rate')
		pl.ylabel('True Positive Rate')
		pl.legend(['fsafe=0.25','fsafe=0.5','fsafe=1','fsafe=2','fsafe=3','fsafe=4','ROC curve','Random'],loc=4,fontsize='small')
		pl.subplot(122)
		for i in range(len(recalls)):
			pl.plot(recalls[i],precisions[i],'.')
		pl.plot(prc[1],prc[0],linewidth=2)
		pl.xlabel('Recall (TPR)')
		pl.ylabel('Precision')
		pl.legend(['fsafe=0.25','fsafe=0.5','fsafe=1','fsafe=2','fsafe=3','fsafe=4', 'PR curve'],fontsize='small',loc=4)
		pl.savefig('day'+str(r)+'_'+str(set)+'_forward'+str(train_days)+'d'+str(test_days)+'_curves.pdf')

f.close()



#####plot AUROC and AUPR for all days
f=open('24_forward10d1_auroc_prec_eps.txt','r')
day=[]
auroc=[]
aupr=[]
for line in f:
	line=line.rstrip(')\n')
	line=line.lstrip('(')
	line=line.split(',')
	day.append(int(line[0])+12)
	auroc.append(float(line[1]))
	aupr.append(float(line[2]))

pl.figure(figsize=(7,4.5))
pl.bar([d-0.3 for d in day],auroc,width=0.4)
pl.bar([d for d in day],aupr,width=0.4,color='green')
pl.xlabel('Test day')
pl.ylim(0, 1.1)
pl.xlim(13.5,28.5)
pl.legend(['AUROC','AUPR'], loc=2)
pl.savefig('au_all_days.pdf')
pl.close()


####find time left to next real fail
##plot time left for TP, TN, FP, FN


def get_real_ttr(machine,time,real_fails):
	d=[float(f[1]) for f in real_fails if float(time)<=float(f[1]) ]
	if(len(d)>0):
		return (min(d)-float(time))
	else:
		return -1

r=15 #this is the best day, for the worst set r=5
epsilon=0.001
lim=0.05#FPR limit

#load all RF results for this day
os=[]
for ns in nsafe:
	print('nsafe',ns)
	for l in range(runs):
		for tc in tree_count:
			c2_acc,c2_sens,c2_spec,c2_prec,F,M,o,y=pkl.load(gzip.open(str(set)+'test0RF'+str(tc)+'_forward'+str(train_days)+'d'+str(test_days)+'dxval'+str(r)+'_corr'+str(cthreshold)+'_nsafe'+str(ns)+'run'+str(l)+'.pkl.gz','rb'))
			if not math.isnan(c2_prec):
				p=c2_prec+epsilon
				c2_acc,c2_sens,c2_spec,c2_prec,F,M,o,y=pkl.load(gzip.open(str(set)+'test1RF'+str(tc)+'_forward'+str(train_days)+'d'+str(test_days)+'dxval'+str(r)+'_corr'+str(cthreshold)+'_nsafe'+str(ns)+'run'+str(l)+'.pkl.gz','rb'))
				os.append([x*p for x in o])

#sum the votes and normalise
o=[sum([x[j] for x in os]) for j in range(len(os[0]))]
m=float(max(o))
o=[x/m for x in o]
print(max(o))

#find recall, precision and threshold for the FPR limit set above
roc=roc_curve(y,o)
dist=[abs(x-lim) for x in roc[0]]
index= dist.index(min(dist))
threshold=roc[2][index]
print(threshold,roc[0][index],roc[1][index])
pr=precision_recall_curve(y,o)
print([pr[0][i] for i in range(len(pr[0])) if pr[1][i]==roc[1][index]])

#apply threshold on score to find TP, FP, TN, FN
o=[int(x>=threshold) for x in o]

#find time to remove due to failure for all points
testTime, testMachine,testTTR=pkl.load(gzip.open(str(set)+'testData'+'_forward'+str(train_days)+'d'+str(test_days)+'dxval'+str(r)+'_corr'+str(cthreshold)+'.pkl.gz','r'))
times=testTTR[1]
real_fails=list(csv.reader(open('real_fails_120min.csv','r')))[1:]
real_fails_dict={}
for f in real_fails:
	old=real_fails_dict.get(float(f[0]),[])
	old.append(f[1:])
	real_fails_dict[float(f[0])]=old

real_ttr=[get_real_ttr(testMachine[1][i],testTime[1][i],real_fails_dict.get(testMachine[1][i],[])) for i in range(len(times))]

#separate into the 4 classes
TP=[real_ttr[i]/3600000000 for i in range(len(times)) if (o[i]==1 and y[i]==1 and real_ttr[i]>-1)]
TN=[real_ttr[i]/3600000000 for i in range(len(times)) if (o[i]==0 and y[i]==0 and real_ttr[i]>-1)] 
FP=[real_ttr[i]/3600000000 for i in range(len(times)) if (o[i]==1 and y[i]==0 and real_ttr[i]>-1)] 
FN=[real_ttr[i]/3600000000 for i in range(len(times)) if (o[i]==0 and y[i]==1and real_ttr[i]>-1)]

#plot the boxplots
plt.figure(figsize=(8,4.4))
plt.subplot(121)
plt.boxplot([TP,FN],sym='b_',notch=True)
plt.text(0.8, -1-((1+max(TP+FN)*1.1)/16.5), 'TP ('+ str(len(TP))+')', bbox=dict(facecolor='white',color='white'))
plt.text(1.8, -1-((1+max(TP+FN)*1.1)/16.5), 'FN ('+ str(len(FN))+')', bbox=dict(facecolor='white',color='white'))
plt.ylabel('Hours to next Remove event')
plt.ylim(-1,max(TP+FN)*1.1)
plt.subplot(122)
plt.boxplot([TN,FP],sym='b_',notch=True)
plt.ylim(-1,max(TN+FP)*1.1)
plt.text(0.8, -1-((1+max(TN+FP)*1.1)/16.5), 'TN ('+ str(len(TN))+')', bbox=dict(facecolor='white',color='white'))
plt.text(1.8, -1-((1+max(TN+FP)*1.1)/16.5), 'FP ('+ str(len(FP))+')', bbox=dict(facecolor='white',color='white'))
plt.savefig('realTTR'+str(r)+'.pdf')#
plt.close()




##########COLLECT PREDICTION RESULTS IN ONE FILE FOR IMPORT BACK TO BIG QUERY
epsilon=0.001
nsafe=[0.25, 0.5, 1,2,3,4]#this is for 24 hrs 120min
train_days=10
test_days=1
step=1
tree_count=[2,3,4,5,6,7,8,9,10,11,12,13,14,15]
cthreshold=0
runs=5
set=24


######create csv file with prediction - machine, time, prediction, real class, ttr

fpr_lims=[0.2,0.1,0.05,0.01]
file=open('prediction_results.csv','w')
out=csv.writer(file)
out.writerows([['time','machine','ttr','prediction','true_class']+[str(lim)+'threshold' for lim in fpr_lims]])
for r in range(2,29-train_days-test_days-1,step): #i is first day used for training
	print('xval',r)
	testTime, testMachine,testTTR = pkl.load(gzip.open(str(set)+'testData_forward'+str(train_days)+'d'+str(test_days)+'dxval'+str(r)+'_corr'+str(cthreshold)+'.pkl.gz','rb'))
	os=[]
	for ns in nsafe:
		print('nsafe',ns)
		for l in range(runs):
			for tc in tree_count:
				c2_acc,c2_sens,c2_spec,c2_prec,F,M,o,y0=pkl.load(gzip.open(str(set)+'test0RF'+str(tc)+'_forward'+str(train_days)+'d'+str(test_days)+'dxval'+str(r)+'_corr'+str(cthreshold)+'_nsafe'+str(ns)+'run'+str(l)+'.pkl.gz','rb'))
				if not math.isnan(c2_prec):
					p=c2_prec+epsilon
					c2_acc,c2_sens,c2_spec,c2_prec,F,M,o,y1=pkl.load(gzip.open(str(set)+'test1RF'+str(tc)+'_forward'+str(train_days)+'d'+str(test_days)+'dxval'+str(r)+'_corr'+str(cthreshold)+'_nsafe'+str(ns)+'run'+str(l)+'.pkl.gz','rb'))
					os.append([x*p for x in o])
	if len(os)>0:
		o=[sum([x[j] for x in os]) for j in range(len(os[0]))]
		m=float(max(o))
		o=[x/m for x in o]
	auroc=roc_auc_score(y1,o)
	roc=roc_curve(y1,o)
	ths=[]
	for lim in fpr_lims:
		dist=[abs(x-lim) for x in roc[0]]
		index= dist.index(min(dist))
		threshold=roc[2][index]
		ths.append(threshold)
	#save table to file
	table=[[testTime[1][i],testMachine[1][i],testTTR[1][i],o[i],y1[i]] + ths for i in range(len(testTime[1]))]
	out.writerows(table)
	file.flush()	

file.close()


######create csv file with benchmark props: start time, end time

file=open('benchmark_test_times.csv','w')
out=csv.writer(file)
out.writerow(['start_time','end_time'])
for r in range(2,29-train_days-test_days,step): #i is first day used for training
	print('xval',r)
	testTime, testMachine,testTTR = pkl.load(gzip.open(str(set)+'testData_forward'+str(train_days)+'d'+str(test_days)+'dxval'+str(r)+'_corr'+str(cthreshold)+'.pkl.gz','rb'))
	out.writerow([min(testTime[1]),max(testTime[1])])	

file.close()

