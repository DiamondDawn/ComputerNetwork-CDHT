import sys
import time
import socket
import threading
Portbase = 45000  # each peer listens to 50000 + i
Host = "127.0.0.1"
Ping_freq = 5.0  # how often ping messages are sent
ACK_MAX = 4  # max amount of ACKS before assumes peer is dead
class Peer:
    def __init__(self, id, pred, succ1, succ2, MSS, drop_pro):
        self.id = id
        self.port = Portbase + id
        self.pred = None
        self.succ1 = succ1
        self.succ2 = succ2
        self.MSS = MSS
        self.drop_pro = drop_pro

        # 5 threads
        # function listening for input
        t0 = threading.Thread(target=self.input_function)
        t0.start()

        # function pinging successor 1
        t1 = threading.Thread(target=self.send_ping, args=(1,))
        t1.daemon = True
        t1.start()

        # function pinging sucessor 2
        t2 = threading.Thread(target=self.send_ping, args=(2,))
        t2.daemon = True
        t2.start()

        # function listening to UDP
        t3 = threading.Thread(target=self.listen_UDP)
        t3.daemon = True
        t3.start()

        # function listening to TCP
        t4 = threading.Thread(target=self.listen_TCP)
        t4.daemon = True
        t4.start()

    # If no ping ACK and a peer is dead, this function updates peers through TCP
    # divided into the two cases for succ1 and succ2
    # succ1 updates immediately moving second forward then pinging that successor
    # succ2 waits for ping freq for the next sucessor to update then contacts for information

    # creates byte array message to incorporate sender ID and extra information when neede
    # function to send_pings via UDP to successors
    def send_ping(self, indicator):  # indicator 1 or 2
        last_seq = 0;
        seqNo = 0
        while True:
            if indicator == 1: targetID = self.succ1
            if indicator == 2: targetID = self.succ2
            if not targetID or targetID == self.id: return

            Sender_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            Sender_socket.settimeout(1.0)

            message = 'pingreq,{},{},{}'.format(self.id, int(indicator), seqNo)
            message = message.encode()
            Sender_socket.sendto(message, (Host, Portbase + targetID))
            # sending packet1:msgtype = pingreq, senderID, indicator,seqNo

            try:
                data, addr = Sender_socket.recvfrom(100)
                dataParts = data.decode().split(',')

                # receiving packet2:msgtype =pingres, responseID,seqNo
                msgtype, responseID, seqNo = dataParts
                if msgtype == 'pingres':
                    print(f"A ping response message was received from Peer {responseID}")
                    last_seq = int(seqNo)
            except socket.timeout:
                last_seq, seqNo = int(last_seq), int(seqNo)
                # evaluates the amount of sequences that have been missed
                if last_seq > seqNo:
                    ACKs = 255 - last_seq + seqNo + 1  # 1 because zero case
                else:
                    ACKs = seqNo - last_seq
                if ACKs:
                    print(
                        f"No ping response from Peer {targetID}, ACK = {ACKs}, sequence gap = [{last_seq}-{seqNo}]")
                if ACKs > ACK_MAX:
                    print(f'Peer {targetID} is no longer alive.')
                    self.kill_peer(indicator)
            # finds next sequence number
            seqNo = (int(seqNo) + 1) % 256
            time.sleep(Ping_freq)

    # if ACK_accumulation > ACK_MAX : self.kill_peer(indicator),ping timeout more than certain times
    def kill_peer(self, succ_number):  # when sucessor is lost
        TCP_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if succ_number == 1:  # first sucessor is lost
            self.succ1 = self.succ2
            TCP_socket.connect((Host, Portbase + int(self.succ2)))
        if succ_number == 2:  # second sucessor is lost
            # time.sleep(Ping_freq)
            TCP_socket.connect((Host, Portbase + int(self.succ1)))

        # sending part:
        message = 'killpeer'
        TCP_socket.send(message.encode())

        # receiving part:if msgtype == 'lostpeer': message = str(self.succ1).
        data = TCP_socket.recvfrom(1024)
        self.succ2 = int(data[0])  # receving the succ of succ
        TCP_socket.close()

        if self.succ1 == self.id:  # special case:no successors
            print("I'm last peer on network, I have no successors")
            self.succ1 = None;
            self.succ2 = None
        else:  # normal case
            print(f'My first successor is now peer {self.succ1}')
            print(f'My second sucessor is now peer {self.succ2}')

    # function to listen to UDP message and return messages
    def listen_UDP(self):
        Reciever_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        Reciever_socket.bind((Host, self.port))

        while True:
            data, addr = Reciever_socket.recvfrom(100)
            # receiving packet1:msgtype =pingreq, senderID, indicator,seqNo
            if not data: break
            dataParts = data.decode().split(',')

            msgtype, senderID, indicator, seqNo = dataParts

            if msgtype == 'pingreq':  # if Ping is from first sucessor, update the predecessor qui
                if int(indicator) == 1:
                    self.pred = int(senderID)  # find a peer's pred
                print(
                    f"A ping request message was received from Peer {senderID}")  # message = self.message_helper(ENCR_MTYPE['pingres'], data[3])
                message = 'pingres,{},{}'.format(self.id, seqNo)
                message = message.encode()
                # sending packet2:msgtype=pingres, responseID,seqNo
                Reciever_socket.sendto(message, addr)

    def listen_TCP(self):
        Server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        Server_socket.bind((Host, self.port))
        Server_socket.listen()
        while True:
            Connection_socket, addr = Server_socket.accept()
            data = Connection_socket.recv(1024).decode()
            dataParts = data.split(',')
            msgtype = dataParts[0]
            # print("dataParts",dataParts)
            try:
                if msgtype == 'killpeer':  # lost peer message received
                    message = str(self.succ1).encode('utf-8')  # return a message containing my successor
                    Connection_socket.send(message)

                # from:message = 'quit,{},{},{}'.format(self.id,self.succ1, self.succ2)
                # quit message received (update sucessors)
                if msgtype == 'quit':
                    msgtype, leaving_peer, new1, new2 = dataParts  # len(dataParts) == 4
                    leaving_peer, new1, new2 = int(leaving_peer), int(new1), int(new2)
                    if leaving_peer == self.succ1:  # case:peer succ1 leave
                        if new1 == self.id:  # special case:last peer
                            # print("I'm last peer on network, I have no successors")
                            self.succ1 = None;
                            self.succ2 = None
                        else:
                            self.succ1 = new1;
                            self.succ2 = new2  # normal case
                        self.send_TCP(data, self.pred)  # sendTCP:tell pred peer has left
                    elif leaving_peer == self.succ2:  # case:peer succ2 leave
                        self.succ2 = new1  # peer succ1 doesn't change, peer succ2 turns to new1
                    if self.succ1 and self.succ2:
                        print(f'My first successor is now peer {self.succ1}')
                        print(f'My second sucessor is now peer {self.succ2}')

                # from: message = 'filereq,{},{},{}'.format(self.id,filehash, filename)
                if msgtype == 'fileres':  # file response from peer
                    msgtype, responsing_peer, filehash, filename = dataParts  # len(dataParts) == 4
                    print(f'Received a response message from peer {int(responsing_peer)}, which has file {filename}')

                # file request from peer
                # from:message = 'filereq,{},{},{}'.format(self.id, filehash, filevalue)
                if msgtype == 'filereq':  # file request from peer
                    msgtype, requesting_peer, filehash, filename = dataParts  # len(dataParts) == 4
                    requesting_peer, filehash, filename = int(requesting_peer), int(filehash), int(filename)
                    print("requesting_peer, filehash, filename", requesting_peer, filehash, filename)
                    print("self.file_locate(filehash)", self.file_locate(filehash))
                    if self.file_locate(filehash) == True:  # found
                        print(f'File {filename} is here.')
                        print(f'A response message, destined for Peer {requesting_peer}, has been sent')
                        message = 'fileres,{},{},{}'.format(self.id, filehash, filename)
                        self.send_TCP(message, requesting_peer)  # tell the requesting_peer that File is here.
                    else:  # forward the message to the next peer
                        print(f'File {filename} is not stored here.')
                        print('File request has been forwarded to my sucessor')
                        self.send_TCP(data, self.succ1)  # doesn't modify data,send it directly to next peer

            except ConnectionRefusedError:
                print("Couldn't connect to peer, please wait until peers update and try again")
            Connection_socket.close()

    # function to sending TCP messages
    def send_TCP(self, msg, receiver):
        while True:
            try:
                TcpSend = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                TcpSend.settimeout(1.0)
                TcpSend.connect((Host, Portbase + int(receiver)))
                TcpSend.send(msg.encode())
                TcpSend.close()
                return True
            except socket.timeout:
                return False

    # if quit is called, it informs the peers of departure
    def peer_departure(self):
        if self.pred == None:
            time.sleep(Ping_freq + 1)  # waits 5 seconds to check if predecessors just havent been updated.
        try:
            print(f'Peer {self.id} will depart from the network')
            print("please allow a few seconds before subsuquent quit functions to allow peers to update")
            message = 'quit,{},{},{}'.format(self.id, self.succ1, self.succ2)
            self.send_TCP(message, self.pred)
            sys.exit(0)
        except (TypeError, ConnectionRefusedError):
            sys.exit(0)

    # ues:if self.file_locate(self, filehash) == True:
    # see whether the hashvalue is closest to the self.id
    def file_locate(self, hashvalue):
       # base case, general case, smaller than smallest peer and case of larger than highest peer,
        if self.id == hashvalue or \
                (self.pred < hashvalue <= self.id) or \
                (self.succ1 < self.id and hashvalue > self.id) or \
                (self.pred > self.id and hashvalue < self.id):
            return True
        else:
            return False
    # if file request is called in input;the beginning of finding file
    def file_request(self, filename):
        filename = int(filename);
        filehash = filename % 256
        if not self.pred:  # if predecessors are None, they need t update
            print("please wait until predecessors update")
        elif self.file_locate(filehash) == True:
            print(f"File {filename} is found here in peer {self.id}")
        else:  # not found in self,forward to self.succ
            try:
                print(f'File {filename} is not stored here.')
                print('File request has been forwarded to my sucessor')
                message = 'filereq,{},{},{}'.format(self.id, filehash, filename)
                # receiving message:msgtype,requesting_peer,filehash, filename
                self.send_TCP(message, self.succ1)  # send filereq to self.succ.
            except ConnectionRefusedError:
                print("Couldn't connect to peer, please wait until peers update and try again")

    # function to constantly listen to input
    def input_function(self):
        while True:
            inp = input("")
            inp.strip()
            if inp == 'quit':
                self.peer_departure()  # start fuc:peer_departure
            elif inp[0:7] == 'request':
                try:
                    filenum = int(inp[8:])
                    if not 0 <= filenum <= 9999:
                        raise ValueError
                    self.file_request(filenum)  # start fuc:peer_departure
                except ValueError:
                    print("incorrect input: file number incorrect")
            else:
                print("Incorrect input: input doesn't match any function")
if __name__ == "__main__":
    ## ensures correct amount of arguments entered (3) and  ##
    ## ensure of the right format (integer) and ensure right range 0 <= arg <= 255 ##
    try:
        if len(sys.argv) != 6:
            print("!!")
            raise ValueError
        for i in range(1, 4):
            sys.argv[i] = int(sys.argv[i])
        for i in range(1, 3):
            if not 0 <= sys.argv[i] <= 255:
                print("??")
                raise ValueError
    except ValueError:
        for i in range(len(sys.argv)):
            print(sys.argv[i])
        print("Incorrect arguments entered")
        sys.exit()
    # init__(self, id, pred, succ1, succ2, MSS, drop_pro):
    peer = Peer(sys.argv[1], None, sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
"""
sys.argv[1]
"peer3" 3 4 5 300 0.3
"peer5" 4 5 8 300 0.3
"""