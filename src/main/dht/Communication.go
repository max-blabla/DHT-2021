package dht

import (
	"fmt"
	log "logrus"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

type NetNode struct{
	server	*rpc.Server

	node	*ChordNode
	//client	rpc.Client
	//conn net.Conn
	Exist bool
	port int
	quitchan chan os.Signal
}



type ActPack struct{
	Key string
	Op int
	Value string
	Start string
	End string
}

type JoinPack struct{
	Successor string
	Predecessor string
	AimAddr string
}

type ReplyPack struct{
	Successors [successorLen]string
	SucSize int
	Predecessor string

	DataMap map[string]string
//	BackUp map[string]string

	Ok bool
	Data string
}


const(
	Put=iota
	Delete
	Get
)

func NewNetNode(port int)* NetNode{
	NewNode:=new(NetNode)
	LocalAddr:=getLocalAddress()
	Addr:=LocalAddr+":"+strconv.Itoa(port)
	NewNode.node=NewChordNode(Addr)
	NewNode.server=rpc.NewServer()

	//NewNode.client=rpc.Client{}
	NewNode.port=port
	//NewNode.conn=nil
	NewNode.quitchan=make(chan os.Signal)
	return NewNode
}

//这两个函数都会启动Server并监听端口
//Client 默认启动了
//发消息时候给目标服务器先Dial后Call就可以了xD

func (this * NetNode)serveBuild(){
	//time.Sleep(0)
	this.Exist=true
	err1:=this.server.Register(this)
	if err1 != nil{
		Fatal("serveBuild:Register:%s",err1)
	}
	tcpAddr,err2:=net.ResolveTCPAddr("tcp",":"+strconv.Itoa(this.port))
	if err2 != nil{
		//这里下个日志包
		//用日志替代
		log.WithFields(
			log.Fields{
				"err": err1,
			},
		).Fatalf("serveBuild:ResolveTCPAddr: %s",err1)
	}
	listen,err3:=net.ListenTCP("tcp",tcpAddr)
	if listen==nil {fmt.Println("?")}
	//listen.AcceptTCP()
	if err3 != nil{
		log.WithFields(
			log.Fields{
				"err": err2,
			},
		).Fatalf("serveBuild:listenTCP: %s",err2)
	}
//	var err4 error
	go func(){
		for this.Exist{
			//加锁
		//	this.node.fingerlock.Lock()
			this.fixFinger()
		//	this.node.fingerlock.Unlock()
			time.Sleep(250*time.Millisecond)
		}
	}()
	go func(){
		for this.Exist{
			this.stablize()
			time.Sleep(250*time.Millisecond)
		}
	}()
	go func(){
		if <-this.quitchan==os.Interrupt{
			listen.Close()
			this.Exist=false
			//this.conn.Close()
			//关闭记录
		}
	}()
	go func(){
		for {
			conn, err4 := listen.Accept()
			if err4 != nil {
				break
			}
			go func() {
				this.server.ServeConn(conn)
			}()

		}
	}()
	//只要客户端关了字节流就会断掉
	//退出判断
	//发出去了收不到？
}

func Fatal(msg string,err error){
	log.WithFields(
		log.Fields{
			"err":err,
		},
		).Fatalf("%s:%s",msg,err,err)
}

func Warn(msg string,err error){
	log.WithFields(
		log.Fields{
			"err":err,
		},
	).Warnf("%s:%s",msg,err,err)
}

//reply 传引用类型！！！
//它外部无需接住err
func (this * NetNode) clientBuild(NetAddr string,logStatus bool,from string,method string)(bool,*rpc.Client){
	//var err1 error
	client,err1:=rpc.Dial("tcp",NetAddr)
	if err1!=nil{
		return false,client
	} else{
		return true,client
	}
}

func (this * NetNode)clientCall(client * rpc.Client,from string,method string,args interface{},reply interface{})bool{
	err2:=client.Call(method,args,reply)
	if err2!=nil{
		return false
	}
	return true
}

//環上所有點後繼數目相

func (this * NetNode) GetPredecessor(_ * string,Predecessor * string)error{
	*Predecessor=this.node.predecessor
	return nil
}

func (this * NetNode) GetSuccessors(_ string,reply * ReplyPack)error{
	this.node.suclock.Lock()
	defer this.node.suclock.Unlock()
	reply.SucSize=this.node.sucsize
	for i:=0;i<reply.SucSize;i++{
		reply.Successors[i]=this.node.successor[i]
	}

	return nil
}

func (this * NetNode) NewPredecessor(args string,reply * string)error{
	* reply=this.node.predecessor
	this.node.predecessor=args
	return nil
}

func (this * NetNode) RenewPredecessor(args string ,_ * string)error{
	this.node.predecessor=args
	return nil
}

type SplitPair struct{
	AimAddr string
	Prev string
}

func (this* NetNode) JoinSplit(args SplitPair, Data *map[string]string)error{
	this.node.datalock.Lock()
	defer this.node.datalock.Unlock()
	HashAddr:=hashString(args.AimAddr)
	HashPrev:=hashString(args.Prev)
	for i,k:=range this.node.data{
		if between(HashPrev,hashString(i),HashAddr,true) {
			(*Data)[i] = k
			delete(this.node.data,i)
		}
	}
	return nil
}

func (this * NetNode) QuitMerge(DataMap map[string]string,_ * string)error{
	this.node.datalock.Lock()
	defer this.node.datalock.Unlock()
	for i,k:=range DataMap{
		this.node.data[i]=k
	}
	return nil
}

func (this * NetNode) AddBackUp(BackUp map[string]string,_ * string)error{
	this.node.backuplock.Lock()
	for i,k:= range BackUp{
		this.node.backup[i]=k
	}
	this.node.backuplock.Unlock()
	return nil
}

func (this * NetNode) FixBackUp(_ string,_ * string)error{
	None:=""
	this.node.backuplock.Lock()
	for i,k:=range this.node.backup{
		this.node.data[i]=k
	}
	this.node.backuplock.Unlock()
	ok,client:=this.clientBuild(this.node.successor[0],true,"FixBackUp","AddBackUp")
	if !ok {return nil}
	this.clientCall(client,"FixBackUp","NetNode.AddBackUp",this.node.backup,&None)
	return nil
}

//FindSuccessor的時候加上guistablize

func (this * NetNode) GuideStablize(_ string,_ * string)error {

	var ok bool
	var client *rpc.Client
	TmpReply := new(ReplyPack)
	None := ""
	//联系后继的后继
	NewSuc := this.node.successor[1]
	if NewSuc != "" {
		ok, client = this.clientBuild(NewSuc, true, "stablize3", "NetNode.GetSuccessors")
		//运气不好可能会出错
		//其它的话就是损失数据备份
		if !ok {
			return nil
		}
		this.clientCall(client, "stablize3", "NetNode.GetSuccessors", None, TmpReply)
		//联系后继的后继
		//告诉它融合备份并且发给后继更新备份
		//更新备份
		ok, client = this.clientBuild(NewSuc, true, "stablize4", "NetNode.FixBackUp")
		this.clientCall(client, "stablize4", "NetNode.FixBackUp", None, &None)
		this.clientCall(client, "stablize5", "NetNode.NewBackUp", this.node.data, &None)
		this.clientCall(client, "stablize6", "NetNode.NewPredecessor", this.node.address, &None)
		this.node.successor[0] = NewSuc
		client.Close()
	} else {
		this.node.sucsize = 0
		this.node.successor[0] = ""
		return nil
	}
	this.node.finger[0] = this.node.successor[0]
	if TmpReply.SucSize == successorLen {TmpReply.SucSize--}
	var top = 1
	this.node.suclock.Lock()
	for i := 1; i <= TmpReply.SucSize; i++ {
		if TmpReply.Successors[i-1] == this.node.address {
			break
		}
		top++
		this.node.successor[i] = TmpReply.Successors[i-1]
	}
	this.node.sucsize = top
	this.node.suclock.Unlock()
	//顺便拿后继的后继
	return nil
}


func (this * NetNode) stablize()error {
	None := new(string)
	if this.node.sucsize==0{return nil
	}
	var client *rpc.Client
	//	var err error
	var ok bool

	TmpReply := new(ReplyPack)

	ok, client = this.clientBuild(this.node.successor[0], false, "stablize2", "NetNode.GetSuccessors")
	if !ok {
		NewSuc:=this.node.successor[1]
		if NewSuc!="" {
			ok, client = this.clientBuild(NewSuc, true, "stablize3", "NetNode.GetSuccessors")
			if !ok {
				return nil
			}
			this.clientCall(client, "stablize3", "NetNode.GetSuccessors", None, TmpReply)
			ok, client = this.clientBuild(NewSuc, true, "stablize4", "NetNode.FixBackUp")
			this.clientCall(client, "stablize4", "NetNode.FixBackUp", None, &None)
			this.clientCall(client, "stablize5", "NetNode.NewBackUp", this.node.data, &None)
			this.clientCall(client,"stablize6","NetNode.NewPredecessor",this.node.address,&None)
			this.node.successor[0] = NewSuc
			client.Close()
		}else{
			this.node.sucsize=0
			this.node.successor[0]=""
			return nil
		}
	} else {
		this.clientCall(client, "stablize2", "NetNode.GetSuccessors", None, TmpReply)
		client.Close()
	}
	this.node.finger[0] = this.node.successor[0]
	if TmpReply.SucSize == successorLen {TmpReply.SucSize--}
	var top = 1
	this.node.suclock.Lock()
	for i := 1; i <= TmpReply.SucSize; i++ {
		if TmpReply.Successors[i-1] == this.node.address {
			break
		}
		top++
		this.node.successor[i] = TmpReply.Successors[i-1]
	}
	this.node.sucsize = top
	this.node.suclock.Unlock()
	//顺便拿后继的后继
	return nil
}


func (this * NetNode) show(f string){
	fmt.Printf("%s:%d\n",f,this.port)
	fmt.Printf("%d\n",this.node.sucsize)
	fmt.Printf("Pre:%s\n",this.node.predecessor)
	fmt.Printf("Suc:\n")
	for i:=0;i<this.node.sucsize;i++{
		fmt.Printf("%s\n",this.node.successor[i])
	}
	fmt.Printf("Data:%d\n",len(this.node.data))
	for _,k:=range this.node.data{
		fmt.Printf("%s ",k)
	}
	fmt.Println()
	fmt.Printf("BackUp:%d\n",len(this.node.backup))
	for _,k:=range this.node.backup{
		fmt.Printf("%s ",k)
	}
	fmt.Println()
	fmt.Println("------------")
}

func (this * NetNode) InsertData(ArgPack ActPack,Pack * ReplyPack)error{
	this.node.datalock.Lock()
	defer this.node.datalock.Unlock()
	Pack.Ok=this.node.InsertData(ArgPack.Key,ArgPack.Value)
	return nil
}

func (this * NetNode) InsertBackUp(ArgPack ActPack,reply * ReplyPack)error{
	this.node.backuplock.Lock()
	defer this.node.backuplock.Unlock()
	reply.Ok=this.node.InsertBackUp(ArgPack.Key,ArgPack.Value)
	return nil
}

func (this * NetNode) DeleteData(ArgPack ActPack,Pack * ReplyPack)error{
	this.node.datalock.Lock()
	defer this.node.datalock.Unlock()
	Pack.Ok=this.node.DeleteData(ArgPack.Key)
	return nil
}

func (this * NetNode) DeleteBackUp(ArgPack ActPack,reply * ReplyPack)error{
	this.node.backuplock.Lock()
	defer this.node.backuplock.Unlock()
	reply.Ok=this.node.DeleteData(ArgPack.Key)
	return nil
}

func (this * NetNode) NewSuccessor(successor string,_ * string)error{
	if successor==""{this.node.sucsize=0}
	this.node.suclock.Lock()
	this.node.successor[0]=successor
	this.node.finger[0]=successor
	this.node.suclock.Unlock()
	return nil
}

func (this * NetNode) FindData(ArgPack ActPack,Pack * ReplyPack)error{
	this.node.datalock.Lock()
	defer this.node.datalock.Unlock()
	Pack.Data,Pack.Ok=this.node.FindData(ArgPack.Key)
	return nil
}

func (this * NetNode) NewBackUp(BackUp map[string]string,_ * string)error{
	this.node.backuplock.Lock()
	this.node.backup=nil
	this.node.backup=make(map[string]string)
	for i,k:=range BackUp{
		this.node.backup[i]=k
	}
	this.node.backuplock.Unlock()
	return nil
}

func (this * NetNode) clientBuildAndCall(NetAddr string,logStatus bool,from string,method string,args interface{},reply interface{}){
	client,err1:=rpc.Dial("tcp",NetAddr)
	if err1!=nil {}
	err2:=client.Call(method,args,reply)
	if err2!=nil {}
	if client!=nil{
		client.Close()
	}
}
//------------------------------------------------------------------------------------------------------------


//-----------------------------------------------------------------------------------------------------------


//-----------------------------------------------------------------------------------------------------------


func (this * NetNode) FindInsertBranch(args JoinPack,reply * ReplyPack)error{
	TmpString:=""
	TmpPrev:=this.node.predecessor
	TmpReply:=new(ReplyPack)
	None:=new(string)

	reply.DataMap=make(map[string]string)

	this.NewPredecessor(args.AimAddr,&TmpString)

	this.clientBuildAndCall(TmpPrev,true,"FindInsertBranch","NetNode.NewSuccessor",args.AimAddr,None)

	this.JoinSplit(SplitPair{args.AimAddr,TmpString},&(reply.DataMap))

	this.GetSuccessors(args.AimAddr,TmpReply)

	//插入的时候也询问后继表吧
	reply.Predecessor = TmpString
	if TmpReply.SucSize==successorLen{
		reply.SucSize=successorLen
		TmpReply.SucSize--
	}else{reply.SucSize=TmpReply.SucSize+1}
	reply.Successors[0]=this.node.address
	for i:=1;i<=TmpReply.SucSize;i++{
		reply.Successors[i]=TmpReply.Successors[i-1]
	}
	return nil

}

func (this * NetNode) FindInsertPlace(args JoinPack,reply * ReplyPack)error{
	if this.node.sucsize==0{
		this.node.predecessor=args.AimAddr
		this.node.successor[0]=args.AimAddr
		this.node.finger[0]=args.AimAddr
		this.node.sucsize=1
		reply.Predecessor=this.node.address
		reply.Successors[0]=this.node.address
		reply.SucSize=1
		reply.Ok=true
		return nil
	}
	TmpReply:=new(ReplyPack)
	var Successors [successorLen]string
	var Cur string
	var Sucsize int
	var None string
	var client *rpc.Client
	this.node.suclock.Lock()
	Sucsize=this.node.sucsize
	for i:=0;i<Sucsize;i++{
		Successors[i]=this.node.successor[i]
	}
	this.node.suclock.Unlock()
	Cur=this.node.address

	reply.DataMap=make(map[string]string)
	HashAddr:=hashString(args.AimAddr)
	out:
		for {
			if between(hashString(Cur), HashAddr, hashString(Successors[0]), true) {
				args.Successor=Successors[0]
				args.Predecessor=Cur
				this.clientBuildAndCall(Successors[0],true,"FindInsertPlace1", "NetNode.FindInsertBranch", args, reply)
				break out
			} else {
				for i := 0; i < Sucsize-1; i++ {
					if between(hashString(Successors[i]), HashAddr, hashString(Successors[i+1]), true) {

						_,client=this.clientBuild(Successors[i+1], true,"FindInsertPlace2", "NetNode.FindInsertBranch")
						this.clientCall(client,"FindInsertPlace2", "NetNode.FindInsertBranch", args, reply)
						if client!=nil{client.Close()}

						break out
					}
				}
			}

			//同理可能失效
			this.clientBuildAndCall(Successors[Sucsize-1],true,"FindInsertPlace3", "NetNode.GetSuccessors", None, TmpReply)

			Cur=Successors[Sucsize-1]
			Sucsize=TmpReply.SucSize
			for i:=0;i<Sucsize;i++{
				Successors[i]=TmpReply.Successors[i]
			}
		}
		reply.Ok=true
	return nil
}

func (this * NetNode) QuitPlace(_ string,_ * string)error {
	None := ""
	if this.node.sucsize == 0 {
		return nil
	}

	var NewPre string
	var NewSuc string
	var client *rpc.Client
	var ok bool
	if this.node.successor[0] == this.node.predecessor {
		NewPre = ""
		NewSuc = ""
	} else {
		NewPre = this.node.predecessor
		NewSuc = this.node.successor[0]
	}
	ok, client = this.clientBuild(this.node.successor[0], true, "QuitPlace", "NetNode.NewPredecessor")
	if !ok {
		return nil
	}
	this.clientCall(client, "QuitPlace", "NetNode.NewPredecessor", NewPre, &None)
	if client != nil {
		client.Close()
	}

	ok, client = this.clientBuild(this.node.predecessor, true, "QuitPlace", "NetNode.NewSuccessor")
	if !ok {
		return nil
	}
	this.clientCall(client, "QuitPlace", "NetNode.NewSuccessor", NewSuc, &None)
	if client != nil {
		client.Close()
	}
	this.clientBuildAndCall(this.node.successor[0], true, "QuitPlace", "NetNode.QuitMerge", this.node.data, &None)

	/*	_,client=this.clientBuild(this.node.successor[0],true,"QuitPlace","QuitMerge")
		this.clientCall(client,"QuitPlace","NetNode.QuitMerge",this.node.data,& None)
		if client!=nil{client.Close()}*/
	return nil
}
func (this * NetNode) FindBranch(args ActPack, reply * ReplyPack)error {
	Suc:=this.node.successor[0]
	//var client *rpc.Client
	//this.clientBuildAndCall(args.End, "FindBranch", "NetNode.GetPredecessor", None, &TmpPrev)
	BackReply:=new(ReplyPack)


	switch args.Op {
	case Put:
		reply.Ok=this.node.InsertData(args.Key,args.Value)
		this.clientBuildAndCall(Suc, true,"FindBranch","NetNode.InsertBackUp", args, BackReply)

	case Delete:
		reply.Ok=this.node.DeleteData(args.Key)
		this.clientBuildAndCall(Suc, true,"FindBranch","NetNode.InsertBackUp", args, BackReply)

	case Get:
		reply.Data,reply.Ok=this.node.FindData(args.Key)
	}

	return nil
}


func (this * NetNode) GetFinger(_ string,reply *[fingerLen]string)error{
	this.node.fingerlock.Lock()
	for i:=0;i<fingerLen;i++{
		(*reply)[i]=this.node.finger[i]
	}
	this.node.fingerlock.Unlock()
	return nil
}

func (this * NetNode) GuideFix(args int,reply * string)error{
	this.node.suclock.Lock()
	Successor := this.node.successor
	//fmt.Printf("W:%d:%s\n",this.port,this.node.finger[this.node.next])
	Sucsize := this.node.sucsize
	this.node.suclock.Unlock()
	Jump := jump(this.node.address, args)
	Standard := hashString(this.node.address)
	None := ""
	Mov:=""
	Cur:=this.node.address
	Reply := new(ReplyPack)
	TmpReply:=new(ReplyPack)
	var client *rpc.Client
	var ok bool
out:
	for {
		for i := 0; i < Sucsize; i++ {
			if Successor[i]==this.node.address {
				this.node.finger[args]=""
				* reply=this.node.address
				return nil
			}
			if between(Standard, Jump, hashString(Successor[i]), true)  {
				this.node.finger[args] = Successor[i]
				break out
			}
		}

		Mov=Successor[Sucsize-1]
		ok,client=this.clientBuild(Mov, true,"GuideFix", "NetNode.GetSuccessors")
		if client!=nil {client.Close()}
		for !ok{
			ok,client=this.clientBuild(Mov,true,"GuideFix2","NetNode.GetSuccessors")
			if client!=nil {client.Close()}
			_,client=this.clientBuild(Cur,true,"GuideFix3","NetNode.GetSuccessors")
			this.clientCall(client,"GuideFix3","NetNode.GetSuccessors",None,TmpReply)
			if client!=nil {client.Close()}

			Mov=TmpReply.Successors[TmpReply.SucSize-1]
			time.Sleep(100 * time.Millisecond)
		}

		this.clientBuildAndCall(Mov,true,"fixFinger", "NetNode.GetSuccessors", None, Reply)

		/*_,client=this.clientBuild(Mov,true,"fixFinger", "NetNode.GetSuccessors")
		this.clientCall(client,   "fixFinger", "NetNode.GetSuccessors", None, Reply)
		if client!=nil{client.Close()}*/

		Sucsize = Reply.SucSize
		Successor = Reply.Successors
		Cur=Mov
	}
	*reply=this.node.finger[args]
	return nil
}

//FindSuccessor 插入肯定會成功對吧
//插入重寫
//找鏈表而不是找跳表
func (this * NetNode) FindSuccessor(args ActPack,reply *ReplyPack)error{
	//fmt.Println("FindSuccessor")

	cur:=this.node.address
	//怎么修finger表??
	//失效了的话，先找到对应可用前继，以及可用后继，然后广播修正表，然后找可用前继的后继来fix对应位置
	//告诉对应点修后继？
	finger:=this.node.GetFinger()

	nxt:=""

	None:=""

	Fixed:=""

	HashAddr:=hashString(args.Key)

	Successor:=this.node.successor

	Treply:=new(ReplyPack)

	var client *rpc.Client

	var ok bool
	//找到直接后继
	if this.node.sucsize==0{
		this.FindBranch(args,reply)
		return nil
	} else{

		for {
			if between(hashString(cur), HashAddr, hashString(Successor[0]), true) {
				ok, client = this.clientBuild(Successor[0], true, "FindSuccessor6", "Dial")
				TmpReply:=new(ReplyPack)
				if !ok{
					_,client=this.clientBuild(cur,true,"FindSuccessor8","NetNode.GuideStablize")
					this.clientCall(client,"FindSuccessor8","NetNode.GuideStablize",None,& None)
					this.clientCall(client,"FindSuccessor9","NetNode.GetSuccessors",None,TmpReply)
					Successor=TmpReply.Successors
					if client!=nil{client.Close()}
				}
				_,client=this.clientBuild(Successor[0],true,"FindSuccessor9","NetNode.FindBranch")
				this.clientCall(client, "FindSuccessor6", "NetNode.FindBranch", args, reply)
				if client != nil {
					client.Close()
				}
				break
			} else {
				var top = fingerLen - 1
				for i := fingerLen - 1; i >= 0; i-- {
					if finger[i] == "" {
						top--
						continue
					} else if finger[i] == Successor[0] {
			//			var Tmpclient *rpc.Client
						ok, client = this.clientBuild(finger[top], true, "FindSuccessor3", "Dial")
						if client != nil {
							client.Close()
						}
						for !ok {

							this.clientBuildAndCall(cur, true, "FindSuccessor4", "NetNode.GuideFix", top, &Fixed)

							finger[top] = Fixed

							ok, client = this.clientBuild(finger[top], true, "FindSuccessor5", "Dial")
							if client != nil {
								client.Close()
							}
							time.Sleep(100 * time.Millisecond)
						}
						nxt = finger[top]
						break
					}
					if between(hashString(finger[i-1]), HashAddr, hashString(finger[i]), true) {
			//			var Tmpclient *rpc.Client
						ok, client = this.clientBuild(finger[i-1], true, "FindSuccessor1.1", "Dial")
						if client != nil {
							client.Close()
						}
						for !ok {
							this.clientBuildAndCall(cur, true,"FindSuccessor1", "NetNode.GuideFix", i-1, &Fixed)

						/*	_, Tmpclient = this.clientBuild(cur, true, "FindSuccessor1.2", "Dial")

							this.clientCall(Tmpclient, "FindSuccessor1", "NetNode.GuideFix", i-1, &Fixed)

							if Tmpclient != nil {
								Tmpclient.Close()
							}*/
							finger[i-1] = Fixed

							ok, client = this.clientBuild(finger[i-1], true, "FindSuccessor2", "Dial")
							if client != nil {
								client.Close()
							}
							time.Sleep(100 * time.Millisecond)
						}

						nxt = finger[i-1]
						break
					}
				}
			}
			//取出下一个直到他们之间不再有差距
			//		this.clientBuildAndCall(nxt,"FindSuccessor","NetNode.GetSuccessors",None,tmpreply)

			ok, client = this.clientBuild(nxt, true, "FindSuccessor7", "NetNode.GetFinger")
			if !ok {
				fmt.Println("W")
			}
			this.clientCall(client, "FindSuccessor7", "NetNode.GetFinger", None, &finger)
			if client != nil {
				client.Close()
			}

			this.clientBuildAndCall(nxt, true,"FindSuccessor7", "NetNode.GetSuccessors", None, Treply)

			/*_, client = this.clientBuild(nxt, true, "FindSuccessor7", "NetNode.GetSuccessors")
			this.clientCall(client, "FindSuccessor7", "NetNode.GetSuccessors", None, Treply)
			if client != nil {
				client.Close()
			}*/
			Successor = Treply.Successors
			cur = nxt

		} //如果没找到则跳转
		return nil
	}
}

func (this * NetNode)InsideCreate(_ ,_ *interface{} )error{
	this.node.CreateNetWork()
	this.serveBuild()
	return nil
}

func (this * NetNode)InsideJoin(NetAddr string,ok * bool)error{


	Pack:=new(ReplyPack)
	Pack.Ok=false
	Pack.DataMap=make(map[string]string)
	args:=new(JoinPack)
	args.AimAddr=this.node.address


	//var client *rpc.Client
	//client:=new(rpc.Client)

	this.serveBuild()

	this.clientBuildAndCall(NetAddr,true,"InsideJoin","NetNode.FindInsertPlace",args,&Pack)

	/*_,client=this.clientBuild(NetAddr,true,"InsideJoin","NetNode.FindInsertPlace")
	this.clientCall(client,  "InsideJoin","NetNode.FindInsertPlace",args,&Pack)
	if client!=nil{client.Close()}*/
	//*ok=this.clientBuildAndCall(NetAddr,"InsideJoin","NetNode.FindInsertPlace",args,&Pack)

	*ok=Pack.Ok
	if *ok{
		//IP:=getLocalAddress()
		this.node.JoinNetWork(Pack.Successors,Pack.Predecessor, Pack.SucSize,&(Pack.DataMap))
	}

	return nil
}

func (this * NetNode)Stable(args,reply *string)error{
	this.stablize()
	return nil
}

type PutPair struct{
	Key,Value string
}
func (this * NetNode)InsidePut(Data PutPair,ok * bool)error{
	ArgPack:=new(ActPack)
	ArgPack.Key=Data.Key
	ArgPack.Value=Data.Value
	ArgPack.Op=Put

	Reply:=new (ReplyPack)
	Reply.Ok=false

	var client *rpc.Client

	_,client=this.clientBuild(this.node.address,true,"InsidePut","NetNode.FindSuccessor")
	this.clientCall(client,  "InsidePut","NetNode.FindSuccessor",ArgPack,&Reply)
	if client!=nil{client.Close()}
	//this.clientBuildAndCall(this.node.address,"InsidePut","NetNode.FindSuccessor",ArgPack,&Reply)

	*ok=Reply.Ok
	return nil
}

func (this * NetNode)InsideDelete(key string,ok *bool)error{
	ArgPack:=new(ActPack)
	ArgPack.Key=key
	ArgPack.Op=Delete

	Reply:=new (ReplyPack)
	Reply.Ok=false

	this.clientBuildAndCall(this.node.address,true,"InsideDelete","NetNode.FindSuccessor",ArgPack,&Reply)

	/*var client *rpc.Client

	_,client=this.clientBuild(this.node.address,true,"InsideDelete","NetNode.FindSuccessor")
	this.clientCall(client,  "InsideDelete","NetNode.FindSuccessor",ArgPack,&Reply)
	if client!=nil{client.Close()}*/
	//this.clientBuildAndCall(this.node.address,"InsideDelete","NetNode.FindSuccessor",ArgPack,&Reply)

	*ok=Reply.Ok
	return nil
}

func (this * NetNode)InsideGet(key string,reply * ReplyPack)error{
	ArgPack:=new(ActPack)
	ArgPack.Key=key
	ArgPack.Op=Get

	Reply:=new (ReplyPack)
	Reply.Ok=false

	this.clientBuildAndCall(this.node.address,true,"InsideGet","NetNode.FindSuccessor",ArgPack,&Reply)

	/*var client *rpc.Client

	_,client=this.clientBuild(this.node.address,true,"InsideGet","NetNode.FindSuccessor")
	this.clientCall(client,  "InsideGet","NetNode.FindSuccessor",ArgPack,&Reply)
	if client!=nil{client.Close()}*/
	//this.clientBuildAndCall(this.node.address,"InsideGet","NetNode.FindSuccessor",ArgPack,&Reply)

	(*reply).Ok=Reply.Ok
	(*reply).Data=Reply.Data
	return nil

}

func (this * NetNode)InsideQuit(_ ,_ *interface{} )error{

	if this.node.status==OnLine {
		None := ""
		this.QuitPlace(None, &None)
		this.node.QuitNetWork()
		this.quitchan <- os.Interrupt
		return nil
	} else{return nil}
}
func (this * NetNode) InsidePing(addr string,ok * bool)error{


	var tmpok bool

	var err error

	var client *rpc.Client

	tmpok,client=this.clientBuild(addr,true,"InsideGet","NetNode.FindSuccessor")

	*ok=tmpok
	if !tmpok {
		Warn("Ping:Dial:%s",err)
	} else{
		//直接关掉就好
		//怕出问题可以让它睡0.001秒
		client.Close()
		//time.sleep(0.01s)
	}
	return nil
}

func (this * NetNode) InsideRun(_ ,_ *interface{} )error{
	this.node.RunMachine()
	return nil
}

func (this * NetNode) InsideForceQuit(_ ,_ *interface{} )error{
	if this.node.status==OnLine {
		this.node.QuitNetWork()
		this.quitchan <- os.Interrupt
		return nil
	} else{return nil}
}

func (this * NetNode)fixFinger() {

	//一次只修一个0.0
	//并且初始置为“
	//找后继
	if this.node.status==OffLine{return
	} else if this.node.sucsize==0{return}
	this.node.suclock.Lock()
	Successor := this.node.successor
	this.node.suclock.Unlock()
	//fmt.Printf("W:%d:%s\n",this.port,this.node.finger[this.node.next])
	Sucsize := this.node.sucsize
	Jump := jump(this.node.address, this.node.next)
	Standard := hashString(this.node.address)
	Mov:=Successor[Sucsize-1]
	Cur:=this.node.address

	//检测Cur合法性(?)
	//(?)什么神经病
	//居然不会强制退出
	Origin := hashString(this.node.finger[this.node.next])
	None := ""
	Reply := new(ReplyPack)
	TmpReply:=new(ReplyPack)
	var client * rpc.Client
	var ok bool

	if this.node.finger[this.node.next]!="" {
		ok,client= this.clientBuild(this.node.finger[this.node.next], false,"fixFinger", "Dial")
		if !ok {
			this.node.fingerlock.Lock()
			this.node.finger[this.node.next] = ""
			this.node.fingerlock.Unlock()
		}
	}
	//,client,_=this.clientBuild(this.node.address,"fixFinger","Dial")
out:
	for {
		for i := 0; i < Sucsize; i++ {
			if Successor[i]==this.node.address {break out}
			if between(Standard, Jump, hashString(Successor[i]), true) {
				if this.node.finger[this.node.next] == "" {
					this.node.fingerlock.Lock()
					this.node.finger[this.node.next] = Successor[i]
					this.node.fingerlock.Unlock()
		//			fmt.Println("?")
					break out
				} else {
					if between(Jump, hashString(Successor[i]), Origin, true) {
						this.node.fingerlock.Lock()
						this.node.finger[this.node.next] = Successor[i]
						this.node.fingerlock.Unlock()
						break out
					} else {
						continue
					}
				}
			}
		}
		if Sucsize==0{break out}
		Mov=Successor[Sucsize-1]
		ok,client=this.clientBuild(Mov, true,"fixFinger4", "NetNode.GetSuccessors")
		if client!=nil {client.Close()}
		for !ok{

			ok,client=this.clientBuild(Cur,true,"fixFinger3","NetNode.GetSuccessors")
			if !ok{return}
			this.clientCall(client,"fixFinger3.2","NetNode.GetSuccessors",None,TmpReply)
			if client!=nil {client.Close()}

			ok,client=this.clientBuild(Mov,true,"fixFinger2","NetNode.GetSuccessors")
			if client!=nil {client.Close()}

			Mov=TmpReply.Successors[TmpReply.SucSize-1]
			time.Sleep(100 * time.Millisecond)
		}

		//this.clientBuildAndCall(Mov, true,"fixFinger5.2", "NetNode.GetSuccessors", None, Reply)

		ok,client=this.clientBuild(Mov, true,"fixFinger5", "NetNode.GetSuccessors")
		if !ok{return}
		this.clientCall(client,   "fixFinger5.2", "NetNode.GetSuccessors", None, Reply)
		if client!=nil{client.Close()}

		//this.clientBuildAndCall(Successor[Sucsize-1], "fixFinger", "NetNode.GetSuccessors", None, Reply)

		Sucsize = Reply.SucSize
		Successor = Reply.Successors
		Cur=Mov
	}
	this.node.next = (this.node.next + 1) % fingerLen
}

func (this * NetNode) Nothing(start string,_*interface{}) error{
	fmt.Println("End")
	return nil
}

func (this * Surface)Fix(){
	for i:=0;i<fingerLen;i++ {
		this.Inside.fixFinger()
		time.Sleep(time.Millisecond)
		//fmt.Println(i)
	}
}

func (this * Surface)FingerPrint(){
	this.Inside.node.fingerlock.Lock()
	defer this.Inside.node.fingerlock.Unlock()
	for i,_:=range this.Inside.node.finger{
		fmt.Printf("%d:%s\n",i,this.Inside.node.finger[i])
	}
}

func (this * Surface)Print(start string)*string{
	this.Inside.show("Print")
	this.Inside.node.suclock.Lock()
	defer  this.Inside.node.suclock.Unlock()
	if this.Inside.node.successor[0]==start||this.Inside.node.address==start{
		return nil
	}
	return & this.Inside.node.successor[0]
}

func getLocalAddress() string {
	var localaddress string

	ifaces, err := net.Interfaces()
	if err != nil {
		panic("init: failed to find network interfaces")
	}

	// find the first non-loopback interface with an IP address
	for _, elt := range ifaces {
		if elt.Flags&net.FlagLoopback == 0 && elt.Flags&net.FlagUp != 0 {
			addrs, err := elt.Addrs()
			if err != nil {
				panic("init: failed to get addresses for network interface")
			}

			for _, addr := range addrs {
				ipnet, ok := addr.(*net.IPNet)
				if ok {
					if ip4 := ipnet.IP.To4(); len(ip4) == net.IPv4len {
						localaddress = ip4.String()
						break
					}
				}
			}
		}
	}
	if localaddress == "" {
		panic("init: failed to find non-loopback interface with valid address on this node")
	}

	return localaddress
}
