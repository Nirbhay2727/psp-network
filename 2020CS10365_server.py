from socket import *
import socket
import os
import threading
import random
import LRU_cache



next="next"
ClientAddr={}              #client address although no specific need as sockets are connected
TCPServerSocketArr={}       #TCP sserver sockets array
TCPServerSocketRecArr={}
TCPServerSocketSendArr={}
UDPServerListenSocketArr={} #UDP server listening socket array
UDPServerReqSocketArr={}    #UDP server request socket array
n=5          #number of clients, can be set as a global variable
bufferSize  = 2048
localIP     = "127.0.0.1"

localPortT   =38700
localPortT2   = 42890
localPortT3   =21760
localPortUR   = 18980
localPortUL   = 23780
clientPortUL=34130
clientPortUR=20570
clientPortT=32550
text_file='A2_small_file.txt'

lock = threading.Lock()

cache=LRU_cache.LRUCache(100)
def init():

    #print("in init")

    TCPServerSocket=socket.socket(family=AF_INET, type=SOCK_STREAM) #TCP welcome socket
    TCPServerSocket.bind((localIP,localPortT))
    TCPServerSocket.listen(n)

    TCPServerSocketRec=socket.socket(family=AF_INET, type=SOCK_STREAM) #TCP welcome socket
    TCPServerSocketRec.bind((localIP,localPortT2))
    TCPServerSocketRec.listen(n)

    TCPServerSocketSend=socket.socket(family=AF_INET, type=SOCK_STREAM) #TCP welcome socket
    TCPServerSocketSend.bind((localIP,localPortT3))
    TCPServerSocketSend.listen(n)

    
    
    for it in range(n):   #loops to establish all three connections and store sockets and addresses

        UDPServerListenSocket=socket.socket(family=AF_INET, type=SOCK_DGRAM)#storing UDP socket
        UDPServerListenSocket.bind((localIP, localPortUL+it))
        UDPServerListenSocketArr[it]=(UDPServerListenSocket)
        UDPServerReqSocket=socket.socket(family=AF_INET, type=SOCK_DGRAM)#storing UDP socket
        UDPServerReqSocket.bind((localIP, localPortUR+it))
        UDPServerReqSocketArr[it]=UDPServerReqSocket
    


    for it in range(n): 

        connectionSocket, TempAddr = TCPServerSocket.accept() #storing TCP socket
        client=int(connectionSocket.recv(bufferSize).decode('utf-8','ignore'))
        TCPServerSocketArr[client]=connectionSocket
        ClientAddr[client]=TempAddr[0]

        connectionSocket2, TempAddr = TCPServerSocketRec.accept() #storing TCP socket
        client=int(connectionSocket2.recv(bufferSize).decode('utf-8','ignore'))
        TCPServerSocketRecArr[client]=connectionSocket2 

        connectionSocket3, TempAddr = TCPServerSocketSend.accept() #storing TCP socket
        client=int(connectionSocket3.recv(bufferSize).decode('utf-8','ignore'))
        TCPServerSocketSendArr[client]=connectionSocket3 
        


    #CHECKPOINT1
    chunk_number=0       #distributing sequence
    with open(text_file) as f:
        chunk_data = f.read(1024)
        while (chunk_data):
            chunk=str(chunk_number)+"*"+chunk_data
            TCPServerSocketArr[chunk_number%n].send(chunk.encode())
            message = TCPServerSocketArr[chunk_number%n].recv(bufferSize).decode('utf-8','ignore')
            chunk_number+=1
            chunk_data = f.read(1024)
            

    
    finish_message="$finished distributing:"+str(chunk_number) #finish distributing sequence
    for i in range(n):#finish message not close connection
         TCPServerSocketArr[i].send(finish_message.encode())


    #sequencial part ends
    #CHECKPOINT2




    def acceptRequest(clientID):#we'll run this in a thread  
            while(True):
                bytesAddressPair = UDPServerListenSocketArr[clientID].recvfrom(bufferSize)
                message = bytesAddressPair[0].decode('utf-8','ignore')
                #print(message)
                address = bytesAddressPair[1]
                requestedChunk=int(message.split("/")[3])
                requestFrom=int(message.split("/")[1])
                chunkToSend=cache.get(requestedChunk)
                
                #  CHECKPOINT3
                if(chunkToSend!="does not exist"):
                    lock.acquire()
                    TCPServerSocketSendArr[clientID].send(chunkToSend.encode())
                    garbage_message = TCPServerSocketSendArr[clientID].recv(bufferSize).decode('utf-8','ignore')
                    lock.release()
                    
                #CHECKPOINT4 see the change of creating a new socket

                else:
                    for i in range(n):
                        if(i!=clientID):
                            #print("sending req to client:"+str(i))
                            UDPServerReqSocketArr[i].sendto(message.encode(),(ClientAddr[clientID],clientPortUL+i))
                            
    #             #CHECKPOINT 5
    


    def accept(clientID):#error here
            while(True):         
                message=TCPServerSocketRecArr[clientID].recv(2048).decode('utf-8','ignore')
                TCPServerSocketRecArr[clientID].send(next.encode())
                if(message==""):
                    continue
                # print()
                # print(message)
                # print()
                messageList=message.split("/")
                requestFrom=int(messageList[1])
                requestedChunkNo=int(messageList[3])
                chunk=(messageList[4])
                #CHECKPOINT6
                temp=cache.cache.get(chunk,"absent")
                if(temp=="absent"):
                    # print("putting chunk "+str(requestedChunkNo)+" in cache and sending back to client")
                    cache.put(requestedChunkNo,chunk)
                    lock.acquire()
                    # print(TCPServerSocketSendArr[requestFrom])
                    # print(chunk)
                    TCPServerSocketSendArr[requestFrom].send(chunk.encode())
                    garbage_message = TCPServerSocketSendArr[requestFrom].recv(bufferSize).decode('utf-8','ignore')
                    lock.release()

    threads=[]
    
    for i in range(n):   # for connection 1
        x = threading.Thread(target=acceptRequest, args=[i])
        threads.append(x)
        x.start()



    for i in range(n):   # for connection 1
        x = threading.Thread(target=accept, args=[i])
        threads.append(x)
        x.start()

init()





























# n=2
# noOfThreads = n
# threads=[]
# for i in range(n):   # for connection 1
#     x = threading.Thread(target=acceptRequest, args=[i])
#     threads.append(x)
#     x.start()

# for i in range(n):   # for connection 1
#     x = threading.Thread(target=accept, args=[i])
#     threads.append(x)
#     x.start()
