library(devtools)
install_github("dgrtwo/broom")
library(broom)
library(ggplot2)
library(dplyr)
library(lmtest)
library(sandwich)

write=TRUE
bigram=TRUE

setwd("C:/Users/mahon/Replication")
reg_data=read.csv('reg_data_2014_9_2021_2.csv')

#Clean shit up, scale variables
reg_data$year=as.factor(substr(reg_data$ï..Month, 1, 4))  
reg_data$month=as.factor(substr(reg_data$ï..Month, 6, 7))  


reg_data$Media3=reg_data$Media3*100
reg_data$Media4=reg_data$Media4*100
reg_data$Media5=reg_data$Media5*100
reg_data$Media6=reg_data$Media6*100
reg_data$Media7=reg_data$Media7*100
reg_data$Pol_bigram=reg_data$Pol_bigram*100
reg_data$Media_bigram=reg_data$Media_bigram*100
reg_data$Pol1=reg_data$Pol1*100
reg_data$Pol2=reg_data$Pol2*100
reg_data$Pol3=reg_data$Pol3*100
reg_data$Pol4=reg_data$Pol4*100
reg_data$Pol5=reg_data$Pol5*100
reg_data$Pol6=reg_data$Pol6*100
reg_data$Pol7=reg_data$Pol7*100
reg_data$Pol8=reg_data$Pol8*100

reg_data$fox_tweets=reg_data$fox_tweets/1000
reg_data$cnn_tweets=reg_data$cnn_tweets/1000
reg_data$total_tweets=reg_data$total_tweets/1000
reg_data$ratio=reg_data$fox_tweets/reg_data$cnn_tweets


if(bigram==TRUE){
  Pol=reg_data$Pol_bigram
  Media=reg_data$Media_bigram}else{
  Pol=reg_data$Pol7
  Media=reg_data$Media6
}

reg_data$Time=reg_data$Time*60
reg_data$Stocks = reg_data$Stocks/1000 

reg_data$Date=as.Date(paste(reg_data$ï..Month,"-01",sep=''))


#Plot the polarization indices
p <- ggplot(reg_data, aes(x=Date, y=Media_bigram)) +
  geom_line(size=1.15) + 
  xlab("Month")+
  ylab("Media Partisanship")+
  ylim(50,80)
  xlim(as.Date('2014-09-01'),as.Date('2021-08-01'))
  
  p=p+ ylab("Media Partisanship")

  p = p+    annotate(geom = "vline",
                 x = c(as.Date('2017-01-01'),as.Date('2020-04-01'), as.Date('2021-01-01')),
                 xintercept = c(as.Date('2017-01-01'),as.Date('2020-04-01'), as.Date('2021-01-01')),
                 linetype = c("dashed", "dashed","dashed")
                 )  
  
  p = p +  annotate(geom = "text",
                label = c('2017-01','2020-04', '2021-01'),
                x = c(as.Date('2017-02-01'),as.Date('2020-05-01') ,as.Date('2021-02-01')),
                y = c(53, 53,53),
                angle = 90, 
                vjust = 1) 
  
  p+scale_x_date(limits = as.Date(c("2014-09-01", "2021-09-01")))
  
p <- ggplot(reg_data, aes(x=Date, y=Pol_bigram)) +
  geom_line(size=1.15) + 
  xlab("Month")+
  ylab("Political Partisanship")
  
p = p+    annotate(geom = "vline",
                   x = c(as.Date('2017-01-01'),as.Date('2020-04-01'), as.Date('2021-01-01')),
                   xintercept = c(as.Date('2017-01-01'),as.Date('2020-04-01'), as.Date('2021-01-01')),
                   linetype = c("dashed", "dashed","dashed"))  

p = p +  annotate(geom = "text",
                  label = c('2017-01','2020-04', '2021-01'),
                  x = c(as.Date('2017-02-01'),as.Date('2020-05-01') ,as.Date('2021-02-01')),
                  y = c(55, 55,55),
                  angle = 90, 
                  vjust = 1) 

p  

  

#Estimate Stage One

stage_one=glm(Media7~Time+Fox_sat+CNN_sat+year, data=reg_data)
summary(stage_one)

stage_one=glm(Media7~Time+ratio+year, data=reg_data)
summary(stage_one)

reg_data$Media5_hat=stage_one$fitted.values
plot(reg_data$Media5_hat,type='l')


#IV Estimation

#No controls
IV_reg=lm(Pol_bigram~Media5_hat,data=reg_data)
if(write){write.csv(as.matrix((coeftest(IV_reg, vcov = vcovHC(IV_reg, type="HC1")))),"no1.csv")}
(coeftest(IV_reg, vcov = vcovHC(IV_reg, type="HC1")))

#No controls, time fixed effects
IV_reg=lm(Pol_bigram~Media5_hat+year,data=reg_data)
if(write){write.csv(as.matrix((coeftest(IV_reg, vcov = vcovHC(IV_reg, type="HC1")))),"no2.csv")}
(coeftest(IV_reg, vcov = vcovHC(IV_reg, type="HC1")))

#Economic controls
IV_reg=lm(Pol_bigram~Media5_hat+Gini2+Urate+Inflation+Interest+GDP+Stocks,data=reg_data)
if(write){write.csv(as.matrix((coeftest(IV_reg, vcov = vcovHC(IV_reg, type="HC1")))),"econ1_new.csv")}
(coeftest(IV_reg, vcov = vcovHC(IV_reg, type="HC1")))

#Political controls
IV_reg=glm(Pol_bigram~Media5_hat+Rep_house+Rep_senate+Rep_prez,data=reg_data)
coeftest(IV_reg, vcov = vcovHC(IV_reg, type="HC1"))
write.csv(as.matrix((coeftest(IV_reg, vcov = vcovHC(IV_reg, type="HC1")))),"polit1_new.csv")

#Econ and Political controls
IV_reg=glm(Pol_bigram~Media5_hat+Gini2+Urate+Inflation+GDP+Stocks+Rep_house+Rep_senate+Rep_prez,data=reg_data)
coeftest(IV_reg, vcov = vcovHC(IV_reg, type="HC1"))
if(write){write.csv(as.matrix((coeftest(IV_reg, vcov = vcovHC(IV_reg, type="HC1")))),"full1_new.csv")}

#Econ controls + time FEs
IV_reg=lm(Pol_bigram~Media5_hat+Gini2+Urate+Interest+Inflation+GDP+Stocks+year,data=reg_data)
(coeftest(IV_reg, vcov = vcovHC(IV_reg, type="HC1")))
if(write){write.csv(as.matrix((coeftest(IV_reg, vcov = vcovHC(IV_reg, type="HC1")))),"econ2_new.csv")}


#Political controls + time FEs
IV_reg=lm(Pol_bigram~Media5_hat++Rep_prez+Rep_house+Rep_senate+year,data=reg_data)
coeftest(IV_reg, vcov = vcovHC(IV_reg, type="HC1"))
if(write){write.csv(as.matrix((coeftest(IV_reg, vcov = vcovHC(IV_reg, type="HC1")))),"polit2_new.csv")}


#Econ and Political controls + yearly fixed effects
IV_reg=glm(Pol_bigram~Media5_hat+Gini2+Urate+Interest+Inflation+GDP+Stocks+Rep_prez+Rep_house+Rep_senate+year,data=reg_data)
coeftest(IV_reg, vcov = vcovHC(IV_reg, type="HC1"))
if(write){write.csv(as.matrix((coeftest(IV_reg, vcov = vcovHC(IV_reg, type="HC1")))),"full2_new.csv")}


IV_reg$coefficients[2]


if(summary(IV_reg)[["coefficients"]][, "t value"][2]>1.96){count=count+1}


####Grid Search on Subsets:
i=1
set_size=70
good_subsets=data.frame(matrix(nrow=0,ncol=3,0))
colnames(good_subsets)=c('Start','End','Count')
N=length(reg_data$Rep_house)

while(set_size-1+i<N+1){
  #Subset reg_data
  reg_sub_data=reg_data[i:(set_size-1+i),]
  
  count=0
  #Estimate Stage One

  stage_one=glm(Media5~Time+ratio+year, data=reg_sub_data)
  reg_sub_data$Media5_hat=stage_one$fitted.values

  #Trace through each model, count=count+1 if significant

  #Economic controls
  IV_reg=lm(Pol6~Media5_hat+Gini2+Urate+Inflation+Interest+GDP+Stocks,data=reg_sub_data)
  if(coeftest(IV_reg, vcov = vcovHC(IV_reg, type="HC1"))[2,3]>1.65){count=count+1}
  
  #Political controls
  IV_reg=glm(Pol6~Media5_hat+Rep_house+Rep_senate+Rep_prez,data=reg_sub_data)
  if(coeftest(IV_reg, vcov = vcovHC(IV_reg, type="HC1"))[2,3]>1.65){count=count+1}
  
  #Econ and Political controls
  IV_reg=glm(Pol6~Media5_hat+Gini2+Urate+Inflation+GDP+Stocks+Rep_house+Rep_senate+Rep_prez,data=reg_sub_data)
  if(coeftest(IV_reg, vcov = vcovHC(IV_reg, type="HC1"))[2,3]>1.65){count=count+1}
  
  #Econ controls + time FEs
  IV_reg=lm(Pol6~Media5_hat+Gini2+Urate+Interest+Inflation+GDP+Stocks+year,data=reg_sub_data)
  if(coeftest(IV_reg, vcov = vcovHC(IV_reg, type="HC1"))[2,3]>1.65){count=count+1}
  
  #Political controls + time FEs
  IV_reg=lm(Pol6~Media5_hat+Rep_house+Rep_senate+year,data=reg_sub_data)
  if(coeftest(IV_reg, vcov = vcovHC(IV_reg, type="HC1"))[2,3]>1.65){count=count+1}
  
  #Econ and Political controls + yearly fixed effects
  IV_reg=glm(Pol6~Media5_hat+Gini2+Urate+Interest+Inflation+GDP+Stocks+Rep_prez+Rep_house+Rep_senate+year,data=reg_sub_data)
  if(coeftest(IV_reg, vcov = vcovHC(IV_reg, type="HC1"))[2,3]>1.65){count=count+1}
  
if(count>3){
  print(count)
  df1=data.frame(matrix(nrow=1,ncol=3,0))
  colnames(df1)=c('Start','End','Count')
  df1=c(reg_data$ï..Month[i],reg_data$ï..Month[set_size-1+i],count)
  good_subsets=rbind(good_subsets, df1)}

  i=i+1
  colnames(good_subsets)=c('Start','End','Count')
  }






#Diagnostics
#1.Normality of Residuals 
#  distribution of studentized residuals

library(MASS)
sresid <- studres(IV_reg)
hist(sresid, freq=FALSE,
     main="Distribution of Studentized Residuals")
xfit<-seq(min(sresid),max(sresid),length=40)
yfit<-dnorm(xfit)
lines(xfit, yfit)

#2. Autocorrelation of residuals


res = IV_reg$res 
n = length(res) 
mod2 = lm(res[-n] ~ res[-1]) 
summary(mod2)

library(car)
#DW test -- lower p-values indicate autocorrelation
durbinWatsonTest(IV_reg)


#Economic controls
IV_reg=lm(Pol6~Media6_hat,data=reg_data)
if(write){write.csv(as.matrix((coeftest(IV_reg, vcov = vcovHC(IV_reg, type="HC1")))),"econ1.csv")}
(coeftest(IV_reg, vcov = vcovHC(IV_reg, type="HC1")))

IV_reg=lm(Pol6~Media6_hat+year,data=reg_data)
if(write){write.csv(as.matrix((coeftest(IV_reg, vcov = vcovHC(IV_reg, type="HC1")))),"econ1.csv")}
(coeftest(IV_reg, vcov = vcovHC(IV_reg, type="HC1")))




