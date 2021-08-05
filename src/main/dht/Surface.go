package dht


type Surface struct{
	 Inside *NetNode
}

func NewSurface(port int)* Surface{
	Ret:=new(Surface)
	Ret.Inside=NewNetNode(port)
	return Ret
}

func (this * Surface)Get(key string)(bool,string){
	data:=new(ReplyPack)
	this.Inside.InsideGet(key,data)
	return data.Ok,data.Data
}

func (this * Surface)Put(key string,value string)bool {
	ok:=new(bool)
	this.Inside.InsidePut(PutPair{key,value},ok)
	return * ok
}

func (this * Surface)Delete(key string)bool {
	ok:=new(bool)
	this.Inside.InsideDelete(key,ok)
	return * ok
}

func (this * Surface)Join(NetAddr string)bool {
	ok:=new(bool)
	this.Inside.InsideJoin(NetAddr,ok)
	return * ok
}

func (this * Surface)Create(){
	this.Inside.InsideCreate(nil,nil)
}


func (this * Surface) Quit(){
	this.Inside.InsideQuit(nil,nil)
}

func (this * Surface) Run(){
	this.Inside.InsideRun(nil,nil)
}

func (this * Surface) Ping(addr string)bool{
	ok:=new(bool)
	this.Inside.InsidePing(addr,ok)
	return * ok
}

func (this * Surface)ForceQuit(){
	this.Inside.InsideForceQuit(nil,nil)
}


