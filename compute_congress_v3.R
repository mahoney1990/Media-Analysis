setwd("C:/Users/mahon")


# Load objects

library(distrom)
library(Matrix)
#filter=read.csv('predictive_vec.csv')
data<-read.csv("Trigram_Counts_congress_2014_2021_v1.csv",fileEncoding = "UTF-8")
n=length(data[1,])

zero_index=which(rowSums(data[,5:n])==0)
data=data[-zero_index,]

data=data[,-1]
n=length(data[1,])
speaker_metadata<-data[,1:3]
colnames(speaker_metadata)=c("Speaker","Date","Party")

records=cbind(speaker_metadata,rowSums(data[,5:n-1]))
write.csv(records,'records.csv')
rm(records)

write.csv(speaker_metadata,"Speaker_metadata_congress.csv")

rownames(data)=speaker_metadata$id
data=data[,-c(1,2,3)]

counts=rowSums(data)
write.csv(cbind(speaker_metadata,counts),'verbosity.csv')

#####DIAGONOSTICS#######

#idx=which(speaker_metadata$Speaker=='Fox Staff')
#data=data[-idx,]
#speaker_metadata=speaker_metadata[-idx,]


#idx=which(speaker_metadata$Speaker=='CNN Staff')
#data=data[-idx,]
#speaker_metadata=speaker_metadata[-idx,]


#idx=which(speaker_metadata$Date=='2015-03')
#data_diag=data[idx,]
#rm_idx=which(as.vector(rowSums(data_diag))>500)

#speaker_metadata_diag=speaker_metadata[idx,]

#data=data[-idx,]
#speaker_metadata=speaker_metadata[-idx,]

#data_diag=data_diag[-rm_idx,]
#speaker_metadata_diag=speaker_metadata_diag[-rm_idx,]

#speaker_metadata=rbind(speaker_metadata,speaker_metadata_diag)
#data=rbind(data,data_diag)

#zero_index=which(rowSums(data)==0)


#########################

for(i in 1:length(data[1,])){data[,i]=as.integer(data[,i])} 
data = Matrix(as.matrix(data),sparse=TRUE )
data[which(data<0)]=-data[which(data<0)]

speaker_metadata$Party[speaker_metadata$Party=="\"Republican\""]=1
speaker_metadata$Party[speaker_metadata$Party=="\"Democrat\""]=0
speaker_metadata$Party=as.numeric(speaker_metadata$Party)

years=speaker_metadata$Date
years=substr(years,1,4)

#elect_index=which(years=="2012"|years=="2016"|years=="2020")
rep_pres_index=which(years=="2017"|years=="2018"|years=="2019"|years=="2020")

speaker_metadata$election=0
speaker_metadata$rep_pres=0

#$election[elect_index]=1
speaker_metadata$rep_pres[rep_pres_index]=1


speaker_metadata$Date=as.character(speaker_metadata$Date)
date_dict=matrix(0,length(unique((speaker_metadata$Date))),2)

date_dict[,1]=unique((speaker_metadata$Date))
date_dict[,2]=seq(from=1,to=length(date_dict[,1]),by=1)
for(i in 1:length(date_dict[,1])){
  
  idx = which(speaker_metadata$Date==date_dict[i,1])
  speaker_metadata$Date[idx]=as.numeric(date_dict[i,2])
  
  
}

speaker_metadata$id=seq(from=1,to=length(speaker_metadata[,1]),by=1)

words=colnames(data)
#words=gsub("X..", "('", words)
#words=gsub("\\....", "', '", words)
#words=gsub("\\..", "')", words)



#speaker_metadata$FoxStaff=0
#speaker_metadata$CNNStaff=0

#idx=which(speaker_metadata$Speaker=='Fox Staff')
#speaker_metadata$FoxStaff[idx]=1
#idx=which(speaker_metadata$Speaker=='CNN Staff')
#speaker_metadata$CNNStaff[idx]=1



C=data
dimnames(C) = list(speaker_metadata$id,words)

colnames(C)=words
rownames(C)=speaker_metadata$id


rm(data)


# Construct penalized estimation inputs
X   <- sparse.model.matrix( ~ 0 + rep_pres + Date+Speaker, data = speaker_metadata)

qx          <- qr(as.matrix(X))
X           <- X[, qx$pivot[1:qx$rank]]
rownames(X) <- speaker_metadata$id
X           <- X[rownames(C), ]


R         <- sparse.model.matrix(~ 0+ rep_pres + Date+Speaker,  data = speaker_metadata)
R          <- R * speaker_metadata$Party
colnames(R) <- paste(colnames(R), 'R', sep = '_')
rownames(R) <- speaker_metadata$id
R           <- R[rownames(C), ]


mu <- log(rowSums(C))
  if(is.na(sum(mu))){stop()
    print("Zero count rows!")}

# Estimate penalized MLE
cl <- makeCluster(2, type = ifelse(.Platform$OS.type == 'unix', 'FORK', 'PSOCK')) 
fit <- dmr(
  cl = cl, 
  covars = cbind(X, R),
  counts = C, 
  mu = mu,
  free = 1:ncol(X),
  fixedcost = 1e-5,
  lambda.start = Inf,
  lambda.min.ratio = 1e-5,
  nlambda = 100,
  standardize = F
)
stopCluster(cl)

rm(years)
rm(rep_pres_index)
rm(zero_index)
rm(cl)
# Get coefficients from penalized MLE
coefs   <- coef(fit, k = log(nrow(X)), corrected = F)
if(is.na(min(coefs))){
  print("Something's fucked up")
  stop()}

coef_mat=as.matrix(coefs)
na_idx=which(colSums(is.na(coef_mat)) == 0)
coef_mat=coef_mat[ , na_idx]
col_size=ncol(coef_mat)
words=words[na_idx]


coefs_X <- coef_mat[colnames(X), ]
coefs_R <- coef_mat[colnames(R), ]
rm(fit)

# Indicate Fox/CNN for speakers and clones
republican <- rowSums(R) > 0
republican <- as.vector(republican)

# Indicate session of congress for speakers and clones
session        <- as.numeric(speaker_metadata$Date)
names(session) <- speaker_metadata$id
session        <- session[rownames(C)]
session        <- as.matrix(session)

# Incremental utility of each phrase for Fox hosts in a given month
# "Phrase-time-specific Party loadings"
phi <- R %*% coefs_R

# Compute utility of speech for Democrats and Republicans
utility_D <- Matrix(cbind(1, X) %*% Matrix(rbind(coef_mat['intercept', ], coefs_X),sparse = TRUE),sparse=TRUE)
#utility_R <- utility_D + phi

# Compute utility of speech for observed speakers and 
# speaker "clones" with the same speech and covariates but the opposite party.
#party_matrix       <- replicate(col_size, rowSums(R))
#party_matrix_clone <- (1 - party_matrix)
#utility_real       <- utility_D + party_matrix * phi
#utility_clone      <- utility_D + (1 - party_matrix) * phi

utility_real       <- utility_D + replicate(col_size, rowSums(R)) * phi
utility_clone      <- utility_D + (1 - replicate(col_size, rowSums(R))) * phi

#rm(party_matrix)

#party_ratio        <- rowSums(exp(utility_D)) / rowSums(exp(utility_D+phi))
#likelihood_ratio   <- rowSums(exp(utility_D)) / rowSums(exp(utility_D+phi)) * exp(phi)
#rho                <- rowSums(exp(utility_D)) / rowSums(exp(utility_D+phi)) * exp(phi / (1+ rowSums(exp(utility_D)) / rowSums(exp(utility_D+phi)) * exp(phi)))
#rho                <- likelihood_ratio / (1 + likelihood_ratio)
rm(coefs)
rm(coefs_R)
rm(coefs_X)
rho                <-(rowSums(exp(utility_D)) / rowSums(exp(utility_D+phi)) * exp(phi))/(1+rowSums(exp(utility_D)) / rowSums(exp(utility_D+phi)) * exp(phi))

rm(utility_D)
#rm(utility_R)
rm(likelihood_ratio)

q_real             <- exp(utility_real)  / rowSums(exp(utility_real))
q_clone            <- exp(utility_clone) / rowSums(exp(utility_clone))
rm(utility_real)
rm(utility_clone)

q_R                <- q_real * republican       + q_clone * (1 - republican) # Real Democrat speakers are Republicans in "clone" matrix
q_D                <- q_real * (1 - republican) + q_clone * republican       # Real Republican speakers are Democrats in "clone" matrix
pi                 <- rowSums(0.5 * q_R * rho + 0.5 * q_D * (1 - rho))

# Compute average partisanship
average_pi       <- tapply(pi, list(session), mean) 
average_pi       <- average_pi[order(as.integer(names(average_pi)))]
average_pi       <- data.frame(session = as.integer(names(average_pi)), average_pi = average_pi)
write.csv(average_pi, file = 'partisanship.csv', row.names = F, quote = F)

plot(average_pi$average_pi,type='l')

rm(q_clone)
rm(q_D)
rm(q_R)
speaker_metadata=read.csv("Speaker_metadata_congress.csv")
for(i in 1:length(date_dict[,1])){
  
  idx = which(speaker_metadata$Date==date_dict[i,1])
  speaker_metadata$Date[idx]=as.numeric(date_dict[i,2])
  
  
}

rm(q_real)
rm(qx)

phidf <- as.matrix(phi)

colnames(phidf) <- words
phi_idx=which(colSums(phidf)!=0)
phidf <-  as.matrix(phi[, phi_idx])

phidf=cbind(speaker_metadata,phidf)
phidf$Date=as.numeric(phidf$Date)

n_months=max(as.numeric((date_dict[,2])))

phidf <- phidf[which(rowSums(phidf[,7:ncol(phidf)]) != 0), ]  # works much faster than below
phidf=as.data.frame(phidf)

write.csv(phidf, file = 'trigram_output_congress.csv', row.names = F, quote = F)
phidf=phidf[,-6]

predictive_terms=data.frame(matrix(nrow=100,ncol=n_months,0))
colnames(predictive_terms)=seq(from=1,to=n_months,by=1)
colnames(predictive_terms)=date_dict[,1]

for(i in 1:n_months){
  df1=phidf[which(phidf[,3]==i),]
  df1=df1[,5:ncol(df1)]
  #df1=as.data.frame(sapply(df1, as.numeric))
  
  test=colSums(df1)
  test=sort(test)
  n_phrases=length(test)
  test_index=which(test==0)
  
  names(test)[test_index]='NA'
  predictive_terms[1:50,i]=names(test[1:50])
  predictive_terms[51:100,i]=names(test[(n_phrases-49):n_phrases])}

write.csv(predictive_terms, file = 'predictive_phrases_congress.csv')


#needed<-which(rownames(phi_cols_data) %in% speaker_metadata$id)
#speaker<-speaker_metadata[needed,]
#phi_cols_data <- cbind(speaker,phi_cols_data)

interp_idx=which(average_pi$average_pi<.515)
for (k in 1:length(interp_idx)){
  average_pi$average_pi[interp_idx[k]]=(average_pi$average_pi[interp_idx[k]-1]
                                        +average_pi$average_pi[interp_idx[k]+1])/2}

write.csv(cbind(average_pi,date_dict),"fuck.csv")

predict_mat=as.matrix(predictive_terms)
predictive_vec=as.vector(predict_mat)

predictive_vec=gsub("X..", "('", predictive_vec)
predictive_vec=gsub("\\....", "', '", predictive_vec)
predictive_vec=gsub("\\..", "')", predictive_vec)

write.csv(predictive_vec,"predictive_vec.csv")
rm(rho)
rm(phi)


View(predictive_terms)
write.csv(colnames(data),"terms.csv")

N=length(phidf[1,])
which.min(colSums(phidf[,5:N]))
scores=sort(colSums(phidf[,5:N]))


write.csv(scores,'scores.csv')





