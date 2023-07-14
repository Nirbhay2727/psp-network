from socket import *
import socket
import threading
import math
import time
import hashlib


hash = hashlib.md5(open("./A2_small_file.txt", 'r').read().encode()).hexdigest()
bufferSize  = 2048
serverName = "127.0.0.1"
serverPort = 38700
serverPort2 = 42890
serverPort3=21760
serverPortUR=18980
serverPortUL=23780
localIP="127.0.0.1"
localPortT=32550
localPortUL=34130
localPortUR=20570
n=5

lock = threading.Lock()
def client(clientID):
   # RTTarr={}
    start=time.time()
    TCPClientWelcomeSocket=socket.socket(family=AF_INET, type=SOCK_STREAM) #TCP welcome socket
    TCPClientWelcomeSocket.bind((localIP,localPortT+clientID))
    TCPClientWelcomeSocket.listen(1)

    UDPClientSocketReq = socket.socket(family=AF_INET, type=SOCK_DGRAM)
    UDPClientSocketReq.bind(("127.0.0.1",localPortUR+clientID))
    UDPClientSocketListen=socket.socket(family=AF_INET, type=SOCK_DGRAM)
    UDPClientSocketListen.bind(("127.0.0.1",localPortUL+clientID))
    print(str(clientID)+":UDP ports for client made")


    TCPClientSocket = socket.socket(AF_INET, SOCK_STREAM)# makes a socket
    TCPClientSocket.connect((serverName,serverPort))
    Message=str(clientID) #requests client id and file size
    TCPClientSocket.send(Message.encode())

    TCPClientSocketSend = socket.socket(AF_INET, SOCK_STREAM)# makes a socket
    TCPClientSocketSend.connect((serverName,serverPort2))
    TCPClientSocketSend.send(Message.encode())

    TCPClientSocketRec = socket.socket(AF_INET, SOCK_STREAM)# makes a socket
    TCPClientSocketRec.connect((serverName,serverPort3))
    TCPClientSocketRec.send(Message.encode())
    #CHECKPOINT1

    chunkDataArr={}
    next = "next"        # sends next message
    totalChunks=0                          
    while(True):

        server_message = TCPClientSocket.recv(2048) #receives chunks
        server_message = server_message.decode('utf-8','ignore') 

        TCPClientSocket.send(next.encode())    #sends next message

        if(server_message[0]=="$"): #checks finish condition
            totalChunks=int(server_message.split(':')[1])
            #TCPClientSocket.close()########IMPORTANT
            break

        chunkNumber=int(server_message.split("*")[0])
        chunkDataArr[chunkNumber]=(server_message) #stores chunk data
        
    # print(chunkDataArr)   

    

    def check():
        text=""
        for i in range(totalChunks):
            text=text+chunkDataArr[i].split("*")[1]
        constructed_hash=hashlib.md5(text.encode()).hexdigest()
        if(hash==constructed_hash):
            print(str(clientID)+"checks out")
            print("Time for client:"+str(clientID)+": "+str(time.time()-start) )
            with open(f'client{clientID}.txt', "w") as file:
                file.write(text)
            #res=0
            #for val in RTTarr.values():
               # res += val
  
            # using len() to get total keys for mean computation
            #res = res / len(RTTarr)
            #print("Avg RTT time for client:"+str(clientID)+"is"+str(res))
        else:
            print(str(clientID)+"doesnt checks out")



    def request():#thinking of repeating this process till we get all, note this is n square
        while(True): #will have to delay here using time.sleep
            flag=0
            for i in range(totalChunks): #file completion condition also and UDP condition
                temp=chunkDataArr.get(i,"absent")
                
                if(temp=="absent"):
                    flag=1
                    to_request="Client/"+str(clientID)+"/requests chunk/"+str(i)
                    # print(to_request)
                    to_request=to_request.encode()
                    #RTTarr[i]=time.time()
                    UDPClientSocketReq.sendto(to_request,(serverName,serverPortUL+clientID))
            time.sleep(0.01)
            if(flag==0):
                break 
        #print("end of request for clientID:"+str(clientID)+"it should have all the chunks, please check")

        #print(str(clientID)+':'+str(chunkDataArr.keys()))
        check()
    #CHECKPOINT3


    def accept():
        print("accept also running for client:"+str(clientID))
        while(True):
                server_message=TCPClientSocketRec.recv(bufferSize).decode('utf-8','ignore')
                # print(server_message)
                TCPClientSocketRec.send(next.encode())
                
                if(server_message==""):
                    continue
                # # print(server_message)
                chunk=server_message.split("*")
                #print(chunk)
                # print(str(clientID)+"accepted"+str(chunk[0]))
                #RTTarr[int(chunk[0])]=time.time()-RTTarr[int(chunk[0])]
                chunkDataArr[int(chunk[0])]=server_message #stores chunk data
                # #break#only for testing purpose
                # #print("accept ended for client:"+str(clientID))
                #print(str(clientID)+':'+str(chunkDataArr.keys()))
    
    #     #tempTCPClientSocket.close()
    #     # print(str(clientID)+':'+str(chunkDataArr.keys()))





    # #CHECKPOINT4 BE AWARE OF PARALLEL THREADS AND REUSE OF SOCKETS

    def acceptReq():
        print("accept request also running for client:"+str(clientID))
        while(True):
            bytesAddressPair = UDPClientSocketListen.recvfrom(bufferSize)
            message = bytesAddressPair[0].decode('utf-8')
            #(message)
            address = bytesAddressPair[1]
            requestFrom=int(message.split('/')[1])
            requestedChunk=int(message.split('/')[3])
            
            #break
            #CHECKPOINT 5
            temp=chunkDataArr.get(requestedChunk,"absent")
            if(temp!="absent"):
                message=message+'/'+temp
                #("in accept request of client"+str(clientID))
                #print("client: "+str(clientID)+" sends chunk "+str(requestedChunk)+"to server")
                TCPClientSocketSend.send(message.encode())
                garbage_message = TCPClientSocketSend.recv(bufferSize).decode('utf-8','ignore')
                #break#for testing
            #CHECKPOINT 6
        
            

    
   
    threads=[]
    x = threading.Thread(target=request)
    threads.append(x)
    x.start()

    x = threading.Thread(target=accept)
    threads.append(x)
    x.start()
    
    x = threading.Thread(target=acceptReq)
    threads.append(x)
    x.start()






noOfThreads = n
threads=[]
for i in range(noOfThreads):   # for connection 1
    x = threading.Thread(target=client, args=[i])
    threads.append(x)
    x.start()





















