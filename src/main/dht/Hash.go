package dht

import (
	"crypto/sha1"
	"math/big"
)

var Two=big.NewInt(2)
var ShaMod=new(big.Int).Exp(big.NewInt(2),big.NewInt(160),nil)


func hashString(address string)* big.Int{
	ShaHash:=sha1.New()
	ShaHash.Write([]byte(address))
	return new(big.Int).SetBytes(ShaHash.Sum(nil))
}


func jump(address string,exponent int)* big.Int{
	Origin := hashString(address)
	Exponent := big.NewInt(int64(exponent))
	Distance := new(big.Int).Exp(Two,Exponent,nil)
	Destination := new(big.Int).Add(Origin,Distance)
	return new(big.Int).Mod(Destination,ShaMod)
}

func between(l,m,r *big.Int,inclusive bool) bool{
	if l.Cmp(r)==0{
		if(l.Cmp(m)==0&&inclusive){
			return true
		}else{	return false}
	} else if l.Cmp(r)<0{
		return ((l.Cmp(m)<0)&&(m.Cmp(r)<0))||(inclusive&&m.Cmp(r)==0)
	} else {
		return (l.Cmp(m)<0)||(m.Cmp(r)<0)||(inclusive&&m.Cmp(r)==0)
	}
}

