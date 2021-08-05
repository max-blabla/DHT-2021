package dht

import "sync"

const fingerLen=160
const successorLen=5

//
//hash(chordNode)>=hash(Key)
const(
	ShutDown=iota
	TurnOn
	OnLine
	OffLine
	Dump
)
type ChordNode struct{
	address string
	data map[string]string
	datalock sync.Mutex
	successor[successorLen]string
	suclock sync.Mutex
	finger [fingerLen]string
	fingerlock sync.Mutex
	backup map[string]string
	backuplock sync.Mutex
	predecessor string
	sucsize int
	status int
	next int
	//next int
}

func NewChordNode(addr string) *ChordNode{
	NewNode:=ChordNode{}
	NewNode.address=addr
	NewNode.sucsize=0
	NewNode.status=ShutDown
	NewNode.predecessor=""
	NewNode.data=make(map[string]string)
	NewNode.backup=make(map[string]string)
	NewNode.next=0
	return &NewNode

}

func (this * ChordNode)RunMachine(){
	this.status=TurnOn
}

func (this * ChordNode)CreateNetWork(){
	this.status=OnLine
	this.predecessor=""
	this.successor[0]=""
	this.sucsize=0;
	this.finger[0]=""
}

func (this * ChordNode)JoinNetWork(successors [successorLen]string,predecessor string,sucsize int,DataMap *map[string]string){
	this.status=OnLine
	this.suclock.Lock()
	for i:=0;i<sucsize;i++{
		this.successor[i]=successors[i]
	}
	this.predecessor=predecessor
	this.sucsize=sucsize
	this.suclock.Unlock()
	this.finger[0]=successors[0]
	this.datalock.Lock()
	for i,k:=range *DataMap{
		this.data[i]=k
	}
	this.datalock.Unlock()

	/*if next==prev{
		this.predecessor=prev.address
		this.successor[0]=prev.address
		prev.predecessor=this.address
		prev.successor[0]=this.address
	} else{
		this.predecessor = prev.address
		next.predecessor = this.address
		this.successor[0]=next.address
		this.successor=append(this.successor,next.successor[:successorLen-1]...)
		prev.successor[0]=this.address
		prev.successor=append(prev.successor,this.successor[:successorLen-1]...)
	}*/
}

/*func (this * ChordNode) NewSuccessors(){

}*/

func (this * ChordNode)QuitNetWork(){
	//所有东西清空再重新开
	this.status=OffLine
	for i,_:=range this.finger{
		this.finger[i]=""
	}
	this.data=nil
	for i,_:=range this.successor{
		this.successor[i]=""
	}
	this.predecessor=""
	this.sucsize=0
	this.data=nil
	this.data=make(map[string]string)
	this.backup=nil
	this.backup=make(map[string]string)
}

func (this * ChordNode) GetFinger()([fingerLen]string){
	this.fingerlock.Lock()
	defer 	this.fingerlock.Unlock()
	return this.finger
}


func (this * ChordNode) InsertData(key,value string)bool{
	_ , ok:=this.data[key]
	if ok {} else{this.data[key]=value}
	return !ok
}

func (this * ChordNode) InsertBackUp(key,value string)bool{
	_,ok:=this.backup[key]
	if ok{}else{this.backup[key]=value}
	return !ok
}

func (this * ChordNode) DeleteData(key string)bool{
	_,ok:=this.data[key]
	if ok{delete(this.data,key)}else{}
	return ok
}

func (this * ChordNode) DeleteBackUp(key string)bool{
	_,ok:=this.backup[key]
	if ok{delete(this.backup,key)}else{}
	return ok
}

func (this * ChordNode) FindData(key string)(string,bool){
	data,ok:=this.data[key]
	return data,ok
}


