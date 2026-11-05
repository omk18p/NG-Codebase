import csv

file=open(r"D:\NG Codebase\Python Fundamentals workboook\data.csv", "r", newline="") 
reader = csv.DictReader(file)

# print(reader)
# for row in reader:
#      print(row)

               
def checkname(row):
     if not isinstance(row["name"], str):
                    print("Name is not String for id:",row["id"])
                    return False
     elif row["name"].strip() == "":
                    print("Name is empty for id: ",row["id"])
                    return False
     return True
def checkage(row):
        id=row["id"]


        try:
                age=int(row["age"])
                if age<=0:
                                print("Age is -ve or zero for id:",id)
                                return False
                elif age>100:  
                                print("Age is >100 for id :",id)  
                                return False
                
        except ValueError:
              print("Age is not Interger for id:",id)
              return False
        else:
                return True

        
def checkscore(row):
        id=row["id"]


        try:
                score=int(row["score"])
                if score<0:
                                print("Score is -ve for id:",id)
                                return False
                elif score>100:  
                                print("Score is >100 for id :",id) 
                                return False 
                
        except ValueError:
              print("Score is not Interger for id:",id)
              return False
        else:
                return True
        
        
def checkemail(row):
        email=row["email"] 
        id=row["id"] 
        if email=="":
                print("Email is Empty for id: ",id)
                return False
        elif '@'not in email:
                print("Email dont contain '@'for id: ",id)
                return False
        elif '.com' not in email:
                print("Email dont have '.com' for id: ",id)   
                return False
        else: 
                return True

def checkid(row):
 try:
        id = int(row["id"])
        if id <= 0:
            print("ID is Negative")
            return False
            
        return True
 except ValueError:
        print("ID is Empty")
        return False

def checkRows(allrows):
 with open(r"D:\NG Codebase\Python Fundamentals workboook\valid_data.csv", "w", newline="") as outfile:   
     writer = csv.DictWriter(outfile, fieldnames=allrows.fieldnames)
     writer.writeheader()

     for row in allrows:
         valid=True
         if checkid(row)==False:
                 valid=False
         if checkname(row)==False:
                valid=False
         if checkage(row)==False:
                valid=False      
         if checkscore(row)==False:
                valid=False  
         if checkemail(row)==False:
              valid=False    
         if valid:
                 print("Valid Record:",row)     
                 writer.writerow(row)  

          
checkRows(reader)