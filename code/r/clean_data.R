#Load the CSV file into data variable one after the other
data = read.csv(file.choose())

#Remove all the messy characters from the body column
data$Body <- gsub("<.*?>","",data$Body)
data$Body <- gsub("[^[:alnum:]]"," ",data$Body)
data$Body <- gsub("[0-9]+", "", data$Body)
data$Body <- gsub("\\n*\\t*\\r*\\s+"," ",data$Body)


#Remove all the messy characters from the Title column
data$Title <- gsub("[^[:alnum:]]"," ",data$Title)
data$Title <- gsub("[0-9]+", "", data$Title)
data$Title <- gsub("\\n*\\t*\\r*\\s+"," ",data$Title)

#Remove all the messy characters from the Tags column
data$Tags <- gsub(",","",data$Tags)
data$Tags <- gsub("\\n*\\t*\\r*\\s+"," ",data$Tags)
data$Tags <- gsub("[<]"," ",data$Tags)
data$Tags <- gsub("[>]"," ",data$Tags)
data$Tags <- gsub("^\\s","",data$Tags)

#Export the cleaned data
write.csv(data,"C:\\Users\\prath\\Desktop\\cleaned_data\\QueryResult4.csv",
          row.names = FALSE)



