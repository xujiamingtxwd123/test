/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"errors"
	"encoding/json"
	"fmt"
	"github.com/hyperledger/fabric/core/chaincode/shim"
	"github.com/op/go-logging"
	"strconv"
)

var log, _ = logging.GetLogger("value_transfer")

type SimpleChaincode struct {
}

// Init takes two arguments, a string and int. The string will be a key with
// the int as a value.
func (t *SimpleChaincode) Init(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
	stub.PutState("Height",[]byte("0"))
	return nil,nil
}


type msg_pay_info struct{
	Payed_mer_id string
	Value string
}
type msg_transfer struct{
	Transfer_type string
	Mer_id string
	Mer_tx_id string
	Order_id string
	Mer_date string
	Pay []msg_pay_info
}

type msg_add_or_del_mer struct{
	Mer_date string
	Mer_id []string

}

type mer_info struct{
	Mer_date string
	Balance string
	Enable string
}

type msg_balance struct {
	Mer_id string
}

type msg_details struct{
	Mer_id string
}
type msg_upload struct {
	Mer_id string
	Order_id string
	Mer_date string
	Upload_tx []msg_pay_info
}

type msg_withdraw struct{
	Mer_id string
	Order_id string
	Mer_date string
	Withdraw_tx []msg_pay_info

}
const(
	
	ERR_ARGS_LENGTH_INVALID = "args length err "
	ERR_UNMARSHAL_INVALID = "unmarshal err "	
	ERR_MARSHAL_INVALID = "marshal err "
	ERR_GET_STATE="get state err "
	ERR_INVOKE_TYPE="invoke type err "
	ERR_RANGE_QUERY_STATE="range query state err "
	ERR_ITER_NEXT = "iter next err "
	ERR_QUERY_TYPE = "query type err "
	ERR_DATA_MSG_EXIST = "data msgs already exist "
	ERR_MER_ID_NOT_EXIST = "mer_id not exist "
	ERR_PAYED_NO_INFO = "payed no info "
	ERR_BALANCE_INSUFFICIENT="balance insufficient"
	ERR_ORDER_ID_CONFLICT="order_id conflict"
	ERR_STRCONV_ATOI = "strconv atoi err "
	ERR_UPLOAD_STRUCT_INVALID="upload struct invalid "
	ERR_WITHDRAW_STRUCT_INVALID="withdraw struct invalid"
	ERR_SYSTEM_UPLOAD="system upload err "

	RET_CODE_OK="0000"
	RET_CODE_ARGS_LENGTH_ERR="0001"
	RET_CODE_ARGS_UNMARSHAL_ERR="0002"
	RET_CODE_DATA_MARSHAL_ERR="0003"
	RET_CODE_GET_STATE_ERR="0004"
	RET_CODE_DATA_ALREADY_EXIST="0005"
	RET_CODE_RANGE_QUERY_STATE="0006"
	RET_CODE_ITER_NEXT_ERR="0007"
	RET_CODE_QUERY_TYPE_ERR="0008"
	RET_CODE_INVOKE_TYPE_ERR="0009"
	RET_CODE_MER_ID_NOT_EXIST="000A"
	RET_CODE_PAYED_NO_INFO="000B"
	RET_CODE_BALANCE_INSUFFICIENT="000C"
	RET_CODE_ORDER_ID_CONFILICT="000D"
	RET_CODE_STRCONV_ATOI="000E"
	RET_CODE_UPLOAD_STRUCT_INVALID="000F"
	RET_CODE_WITHDRAW_STRUCT_INVALID="0010"
	RET_CODE_SYSTEM_UPLOAD_ERR="0011"
)
type Ccresponse struct {
	Retcode string 
	Retmsg string 
}
func getRetMsg(ret_code string, ret_msg string) ([]byte) {
	var res = Ccresponse{
		Retcode: ret_code,
		Retmsg: ret_msg,
	}

	resByte, err := json.Marshal(res)
	if (err != nil) {
		log.Errorf("#marshal ccresponse err [retcode=%s], [retmsg=%s]", ret_code, ret_msg)
		return []byte("{\"ret_code\":500,\"ret_msg\":\"marshal ccresponse err\"}")
	}

	return resByte
}

func checkMerExist(stub shim.ChaincodeStubInterface,mer_id string) ([]byte, error) {
	log.Infof("checkMerExist") 
	mer_id_exist,err:= stub.GetState(mer_id)
         if err!= nil{
		log.Errorf(ERR_GET_STATE+"%s\n",err)
                return getRetMsg(RET_CODE_GET_STATE_ERR,ERR_GET_STATE+err.Error()),errors.New(ERR_GET_STATE+err.Error())
         }

	if mer_id_exist == nil{
		log.Infof("checkMerExist not exist")
		return []byte("false"),nil
	}else{
		exist := mer_info{}
       	 	err_unmarshal:=json.Unmarshal(mer_id_exist,&exist)
        	if err_unmarshal != nil{
                	log.Errorf(ERR_UNMARSHAL_INVALID+"%s\n",err_unmarshal)
                	return getRetMsg(RET_CODE_ARGS_UNMARSHAL_ERR,ERR_UNMARSHAL_INVALID+err_unmarshal.Error()),errors.New(ERR_UNMARSHAL_INVALID+err_unmarshal.Error())
        	}
		log.Infof("checkMerExit exist %s",exist.Enable)
		return []byte( exist.Enable),nil 
	}
}
func getBalance(stub shim.ChaincodeStubInterface,mer_id string) ([]byte,error){
	log.Infof("getBalance")
        var err error
        var msgValid []byte
        msgValid,err = checkMerExist(stub,mer_id)
        if err != nil{
                return msgValid,err
        }
        if string(msgValid) == "false"{
                log.Errorf(ERR_MER_ID_NOT_EXIST+" %s\n",mer_id)
                return getRetMsg(RET_CODE_MER_ID_NOT_EXIST,ERR_MER_ID_NOT_EXIST+mer_id),errors.New(ERR_MER_ID_NOT_EXIST+mer_id)
        }
	mer_info_byte_array,err:=stub.GetState(mer_id)
	if err!= nil{
                log.Errorf(ERR_GET_STATE+"%s\n",err)
                return getRetMsg(RET_CODE_GET_STATE_ERR,ERR_GET_STATE+err.Error()),errors.New(ERR_GET_STATE+err.Error())
        }
	balance := mer_info{}
	err_unmarshal:=json.Unmarshal(mer_info_byte_array,&balance)
	if err_unmarshal != nil{
        	log.Errorf(ERR_UNMARSHAL_INVALID+"%s\n",err_unmarshal)
                return getRetMsg(RET_CODE_ARGS_UNMARSHAL_ERR,ERR_UNMARSHAL_INVALID+err_unmarshal.Error()),errors.New(ERR_UNMARSHAL_INVALID+err_unmarshal.Error())
        }
	log.Infof("mer_id:%s, balance:%s\n",mer_id, balance.Balance)
	return []byte(balance.Balance),nil

}
func checkTransferValue(stub shim.ChaincodeStubInterface,tran_data *msg_transfer) ([]byte,error){
	log.Infof("checkTransferValue")
	var err error
	var msgValid []byte
	balance,err:=getBalance(stub,tran_data.Mer_id)
	if err != nil{
		return balance,err
	}

	var balance_value float64
	balance_value,_ = strconv.ParseFloat(string(balance), 64)

	len := len(tran_data.Pay)
	if len == 0{
		log.Errorf(ERR_PAYED_NO_INFO)
                return getRetMsg(RET_CODE_PAYED_NO_INFO,ERR_PAYED_NO_INFO),errors.New(ERR_PAYED_NO_INFO)
	}
	var pay_value float64 = 0
	var system_upload string = "false"	


	for i:=0;i<len;i++ {
	      msgValid,err = checkMerExist(stub,tran_data.Pay[i].Payed_mer_id)
	        if err != nil{ 
        	        return msgValid,err
        	}       
        	if string(msgValid) == "false"{
                	log.Errorf(ERR_MER_ID_NOT_EXIST+" %s\n",tran_data.Pay[i].Payed_mer_id)
                	return getRetMsg(RET_CODE_MER_ID_NOT_EXIST+tran_data.Pay[i].Payed_mer_id,ERR_MER_ID_NOT_EXIST+tran_data.Pay[i].Payed_mer_id),errors.New(ERR_MER_ID_NOT_EXIST+tran_data.Pay[i].Payed_mer_id)
        	}   
		
		//upload
		if tran_data.Mer_id == "system" && tran_data.Pay[i].Payed_mer_id == "system"{
			if (system_upload == "false") && (i != 0){
				log.Errorf(ERR_SYSTEM_UPLOAD)
				return getRetMsg(RET_CODE_SYSTEM_UPLOAD_ERR,ERR_SYSTEM_UPLOAD),errors.New(ERR_SYSTEM_UPLOAD)
			}
			log.Infof("checkTransferValue system upload %s",tran_data.Pay[i].Value)
			system_upload = "true"
		}		

		value,_:=strconv.ParseFloat(tran_data.Pay[i].Value, 64)
		log.Infof("checkTransferValue to mer_id: %s, value:%s",tran_data.Pay[i].Payed_mer_id,tran_data.Pay[i].Value)
		pay_value += value
	}

	if (system_upload == "false") && (balance_value < pay_value){
		log.Errorf(ERR_BALANCE_INSUFFICIENT)
		return getRetMsg(RET_CODE_BALANCE_INSUFFICIENT,ERR_BALANCE_INSUFFICIENT),errors.New(ERR_BALANCE_INSUFFICIENT)
	}

	return []byte(system_upload),nil
}

func checkTransferMsg(stub shim.ChaincodeStubInterface,tran_data *msg_transfer) ([]byte,error){
	log.Infof("checkTransferMsg")
	msgValid,err:=checkTransferValue(stub,tran_data)
	if err != nil{
		return msgValid,err
	}
 	//len := len(tran_data.Pay)

        if tran_data.Mer_id != "system"{
		//check order_id,here only check order_id todo check payed same mer_id
		iter,err :=stub.RangeQueryState(tran_data.Mer_tx_id+"1"+tran_data.Order_id+"0",tran_data.Mer_tx_id+"1"+tran_data.Order_id+":")
                if err != nil{
                        log.Errorf(ERR_RANGE_QUERY_STATE+"%s\n",err)
                        return getRetMsg(RET_CODE_RANGE_QUERY_STATE,ERR_RANGE_QUERY_STATE+err.Error()), errors.New(ERR_RANGE_QUERY_STATE+err.Error())
                }
		defer iter.Close()
		if iter.HasNext() == true{
			log.Errorf(ERR_ORDER_ID_CONFLICT)
                        return getRetMsg(RET_CODE_ORDER_ID_CONFILICT,ERR_ORDER_ID_CONFLICT), errors.New(ERR_ORDER_ID_CONFLICT)
		}
	}
	return msgValid,nil
}

const(
	TYPE_ADD = "ADD"
	TYPE_SUB = "SUB"

)
func setBalance(stub shim.ChaincodeStubInterface,mer_id string, value string, op string) ([]byte,error){
      	log.Infof("setBalance")
	mer_byte_array,err:=stub.GetState(mer_id)
        if err!= nil{
                 log.Errorf(ERR_GET_STATE+"%s\n",err)
                 return getRetMsg(RET_CODE_GET_STATE_ERR,ERR_GET_STATE+err.Error()),errors.New(ERR_GET_STATE+err.Error())
        }
	log.Infof("set Balance mer_id:%s, data:%s",mer_id, mer_byte_array)
	mer_data_info := mer_info{}
	err_unmarshal:=json.Unmarshal(mer_byte_array,&mer_data_info)
	if err_unmarshal != nil{
        	log.Errorf(ERR_UNMARSHAL_INVALID+"%s\n",err_unmarshal)
                return getRetMsg(RET_CODE_ARGS_UNMARSHAL_ERR,ERR_UNMARSHAL_INVALID+err_unmarshal.Error()),errors.New(ERR_UNMARSHAL_INVALID+err_unmarshal.Error())
        }
	balance,_ := strconv.ParseFloat(mer_data_info.Balance, 64)
	op_value,_ := strconv.ParseFloat(value, 64)

	log.Infof("setBalance balance:%f op:%s op_value:%f",balance,op,op_value)
	if op == TYPE_ADD{
		balance += op_value
	}else{
		balance -= op_value
	}
	mer_data_info.Balance = strconv.FormatFloat(balance,'f',5,64)
	data_byte_array,err_marshal:=json.Marshal(mer_data_info)
	log.Infof("set Balance result mer_id:%s, data:%s",mer_id, mer_data_info)
 
        if err_marshal != nil{
                 log.Errorf(ERR_MARSHAL_INVALID+"%s\n",err_marshal)
                 return getRetMsg(RET_CODE_DATA_MARSHAL_ERR,ERR_MARSHAL_INVALID+err_marshal.Error()),errors.New(ERR_MARSHAL_INVALID+err_marshal.Error())
        }

	stub.PutState(mer_id,data_byte_array)	
	return nil,nil
}


// todo  may create DetailList save space
func addDetail(stub shim.ChaincodeStubInterface, tran_data *msg_transfer, system_upload string) ([]byte,error){
	log.Infof("addDetail")
	height,err:=stub.GetState("Height")
	if err!= nil{
                 log.Errorf(ERR_GET_STATE+"%s\n",err)
                 return getRetMsg(RET_CODE_GET_STATE_ERR,ERR_GET_STATE+err.Error()),errors.New(ERR_GET_STATE+err.Error())
        }
	height_int,err_atoi:=strconv.Atoi(string(height))

	log.Infof("addDetail current height :%d",height_int)
	if err!= nil{
              log.Errorf(ERR_STRCONV_ATOI+"%s\n",err)
              return getRetMsg(RET_CODE_STRCONV_ATOI,err_atoi.Error()),errors.New(ERR_STRCONV_ATOI+err_atoi.Error())
         }

	height_string:=strconv.Itoa(height_int+1)
	log.Infof("addDetail new Height %s",height_string)
	stub.PutState("Height",[]byte(height_string))
	
	pay_key:=tran_data.Mer_tx_id+"1"+tran_data.Order_id+string(height) 
	data_byte_array,err_marshal:=json.Marshal(tran_data)
	
	if err_marshal != nil{
		log.Errorf(ERR_MARSHAL_INVALID+"%s\n",err_marshal)
		return getRetMsg(RET_CODE_DATA_MARSHAL_ERR,ERR_MARSHAL_INVALID+err_marshal.Error()),errors.New(ERR_MARSHAL_INVALID+err_marshal.Error())
	}

//consume

	pay_date_key:=tran_data.Mer_tx_id+"1"+tran_data.Mer_date+tran_data.Order_id+string(height) 

	
	if system_upload == "false"{
		log.Infof("addDetail put-state pay_key:%s data:%s",pay_key,data_byte_array)
		log.Infof("addDetail put-state pay_datekey:%s data:%s",pay_date_key,data_byte_array)
		stub.PutState(pay_key,data_byte_array)
		stub.PutState(pay_date_key,data_byte_array)
	}else{
		log.Infof("addDetail system upload")
	}



	var balance_value float64 = 0

	len := len(tran_data.Pay)

	for i:=0;i<len;i++{
		payed_key := tran_data.Pay[i].Payed_mer_id + "0"+ tran_data.Mer_tx_id+tran_data.Order_id
		stub.PutState(payed_key,data_byte_array)

		log.Infof("addDetail put-state payed_key:%s data:%s",payed_key,data_byte_array)
		
		payed_date_key := tran_data.Pay[i].Payed_mer_id + "0"+ tran_data.Mer_date+tran_data.Mer_tx_id+tran_data.Order_id
		stub.PutState(payed_date_key,data_byte_array)
		log.Infof("addDetail put-state payed_date_key:%s data:%s",payed_key,data_byte_array)
		value,_ := strconv.ParseFloat(string(tran_data.Pay[i].Value), 64)
		balance_value += value
		result,err_balance:=setBalance(stub,tran_data.Pay[i].Payed_mer_id,tran_data.Pay[i].Value,TYPE_ADD)
		if err_balance != nil{
			return result,err_balance
		}
	}
	var result []byte
	var err_balance error
	if system_upload == "false"{
		result,err_balance =setBalance(stub,tran_data.Mer_id,strconv.FormatFloat(balance_value,'f',5,64),TYPE_SUB)
	}
	log.Infof("addDetail Ok!")
	return result,err_balance
}

func transferProcess(stub shim.ChaincodeStubInterface,tran_data *msg_transfer) ([]byte,error){
	msgValid,err:=checkTransferMsg(stub,tran_data)
	if err != nil{
		return msgValid,err
	}
	//do transfer
	msgValid,err=addDetail(stub,tran_data,string(msgValid))
	if err != nil{
		return msgValid,err
	}
	return nil,nil
}
// Invoke queries another chaincode and updates its own state
func (t *SimpleChaincode) Invoke(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
		if len(args) != 1{
                        log.Errorf(ERR_ARGS_LENGTH_INVALID)
                        return getRetMsg(RET_CODE_ARGS_LENGTH_ERR,ERR_ARGS_LENGTH_INVALID),errors.New(ERR_ARGS_LENGTH_INVALID)
                }
		switch function {
			case "add_mer":
				log.Infof("begin add_mer\n")
				mer_data := msg_add_or_del_mer{}
				err_unmarshal:=json.Unmarshal([]byte(args[0]),&mer_data)
				if err_unmarshal != nil{
                        		log.Errorf(ERR_UNMARSHAL_INVALID+"%s\n",err_unmarshal)
                        		return getRetMsg(RET_CODE_ARGS_UNMARSHAL_ERR,ERR_UNMARSHAL_INVALID),errors.New(ERR_UNMARSHAL_INVALID)
               	 		}
				length := len(mer_data.Mer_id)

				mer_data_info := mer_info{}
				mer_data_info.Mer_date = mer_data.Mer_date
				mer_data_info.Balance="0"
				mer_data_info.Enable = "true"

				for i:=0;i < length;i++{
					mer_id_exist,err := checkMerExist(stub,mer_data.Mer_id[i])
					if err != nil {
						return mer_id_exist,err
					}
					if string(mer_id_exist) == "true"{
						continue
					}
					data_byte_array,err_marshal:=json.Marshal(mer_data_info)
					if err_marshal != nil{
						log.Errorf(ERR_MARSHAL_INVALID+"%s\n",err_marshal)
						return getRetMsg(RET_CODE_DATA_MARSHAL_ERR,ERR_MARSHAL_INVALID+err_marshal.Error()),errors.New(ERR_MARSHAL_INVALID+err_marshal.Error())
					}
					log.Infof("add_mer:%s data:%s\n",mer_data.Mer_id[i],data_byte_array)
					stub.PutState(mer_data.Mer_id[i],data_byte_array)
				}

			case "del_mer":
				log.Infof("del_mer")
				mer_data := msg_add_or_del_mer{}
                                err_unmarshal:=json.Unmarshal([]byte(args[0]),&mer_data)
                                if err_unmarshal != nil{
                                        log.Errorf(ERR_UNMARSHAL_INVALID+"%s\n",err_unmarshal)
                                        return getRetMsg(RET_CODE_ARGS_UNMARSHAL_ERR,ERR_UNMARSHAL_INVALID),errors.New(ERR_UNMARSHAL_INVALID)
                                }
                                length := len(mer_data.Mer_id)
                                for i:=0;i < length;i++{
                                        mer_id_exist,err := checkMerExist(stub,mer_data.Mer_id[i])
					if err != nil{
						return mer_id_exist,err
					}
					if string(mer_id_exist) == "true"{
						mer_data_info := mer_info{}
						data_byte_array,err:= stub.GetState(mer_data.Mer_id[i])
						if err!= nil{
							log.Errorf(ERR_GET_STATE+"%s\n",err)
							return getRetMsg(RET_CODE_GET_STATE_ERR,ERR_GET_STATE+err.Error()),errors.New(ERR_GET_STATE+err.Error())
						}
						json.Unmarshal(data_byte_array,&mer_data_info)
						mer_data_info.Enable = "false"
						data_byte_array,err=json.Marshal(mer_data_info)
						if err != nil{
							log.Errorf(ERR_MARSHAL_INVALID+"%s\n",err)
							return getRetMsg(RET_CODE_DATA_MARSHAL_ERR,ERR_MARSHAL_INVALID+err.Error()),errors.New(ERR_MARSHAL_INVALID+err.Error())
						}
						log.Infof("del_mer:%s data:%s",mer_data.Mer_id[i],data_byte_array)
						stub.PutState(mer_data.Mer_id[i],data_byte_array)
					}
                                }
				return nil,nil
			case "upload_mer":
				log.Infof("upload_mer")
				upload_data := msg_upload{}
				err_unmarshal:=json.Unmarshal([]byte(args[0]),&upload_data)
				if err_unmarshal != nil{
					log.Errorf(ERR_UNMARSHAL_INVALID+"%s\n",err_unmarshal)
					return getRetMsg(RET_CODE_ARGS_UNMARSHAL_ERR,ERR_UNMARSHAL_INVALID),errors.New(ERR_UNMARSHAL_INVALID)
				}

				if upload_data.Mer_id != "system" || len(upload_data.Upload_tx) != 1{
					log.Errorf(ERR_UPLOAD_STRUCT_INVALID+"%s\n",err_unmarshal)
					return getRetMsg(RET_CODE_UPLOAD_STRUCT_INVALID,ERR_UPLOAD_STRUCT_INVALID),errors.New(ERR_UPLOAD_STRUCT_INVALID)
				}
				tran_data := msg_transfer{}
				tran_data.Transfer_type="upload"
				tran_data.Mer_id = upload_data.Mer_id
				tran_data.Mer_date = upload_data.Mer_date
				tran_data.Order_id = upload_data.Order_id
				tran_data.Mer_tx_id = upload_data.Upload_tx[0].Payed_mer_id
				tran_data.Pay = upload_data.Upload_tx
				log.Infof("upload_mer op %s",tran_data)

				result,err_transfer:=transferProcess(stub,&tran_data)
				if err_transfer != nil{
					return result,err_transfer
				}

			case "withdraw_mer":
				log.Infof("withdraw_mer")
				withdraw_data := msg_withdraw{}
				err_unmarshal:=json.Unmarshal([]byte(args[0]),&withdraw_data)
					if err_unmarshal != nil{
					log.Errorf(ERR_UNMARSHAL_INVALID+"%s\n",err_unmarshal)
					return getRetMsg(RET_CODE_ARGS_UNMARSHAL_ERR,ERR_UNMARSHAL_INVALID),errors.New(ERR_UNMARSHAL_INVALID)
				}

				if withdraw_data.Withdraw_tx[0].Payed_mer_id != "system" || len(withdraw_data.Withdraw_tx) != 1{
					log.Errorf(ERR_WITHDRAW_STRUCT_INVALID+"%s\n",err_unmarshal)
					return getRetMsg(RET_CODE_WITHDRAW_STRUCT_INVALID,ERR_WITHDRAW_STRUCT_INVALID),errors.New(ERR_WITHDRAW_STRUCT_INVALID)
				}
				tran_data := msg_transfer{}
				tran_data.Transfer_type = "withdraw"
				tran_data.Mer_id = withdraw_data.Mer_id
				tran_data.Mer_date = withdraw_data.Mer_date
				tran_data.Order_id = withdraw_data.Order_id
				tran_data.Mer_tx_id = withdraw_data.Mer_id
				tran_data.Pay = withdraw_data.Withdraw_tx
				log.Infof("withdraw_mer op %s",tran_data)
				result,err_transfer:=transferProcess(stub,&tran_data)
				if err_transfer != nil{
					return result,err_transfer
				}
			
			case "transfer":
				tran_data := msg_transfer{}
				err_unmarshal:=json.Unmarshal([]byte(args[0]),&tran_data)
                                if err_unmarshal != nil{
                                        log.Errorf(ERR_UNMARSHAL_INVALID+"%s\n",err_unmarshal)
                                        return getRetMsg(RET_CODE_ARGS_UNMARSHAL_ERR,ERR_UNMARSHAL_INVALID),errors.New(ERR_UNMARSHAL_INVALID)
                                }
				tran_data.Transfer_type="transfer"
				log.Infof("transfer %s",tran_data)
				result,err_transfer:=transferProcess(stub,&tran_data)
				if err_transfer != nil{
					return result,err_transfer
				}

			default:
	                	log.Errorf(ERR_INVOKE_TYPE)
	                	return getRetMsg(RET_CODE_INVOKE_TYPE_ERR,ERR_INVOKE_TYPE),errors.New(ERR_INVOKE_TYPE)

	}

	return nil,nil
}
type msg_query_date struct {
	Mer_id string
	Begin_mer_date string
	End_mer_date string
}
type msg_query_order_id struct {
	Mer_id string
	Begin_order_id string
	End_order_id string
}
func queryIter(stub shim.ChaincodeStubInterface,begin_con string,end_con string) (interface{},error){

	data_array := make([]msg_transfer, 0)
	iter,err :=stub.RangeQueryState(begin_con,end_con)

	if err != nil{
		log.Errorf(ERR_RANGE_QUERY_STATE+"%s\n",err)
		return getRetMsg(RET_CODE_RANGE_QUERY_STATE,ERR_RANGE_QUERY_STATE+err.Error()), errors.New(ERR_RANGE_QUERY_STATE+err.Error())
	}
	defer iter.Close()

	for iter.HasNext(){
		_,value,err:=iter.Next()
		if err != nil{
			log.Errorf(ERR_ITER_NEXT+"%s\n",err)
			return getRetMsg(RET_CODE_ITER_NEXT_ERR,ERR_ITER_NEXT + err.Error()),errors.New(ERR_ITER_NEXT + err.Error())
		}
		data_msg := msg_transfer{}
		err_unmarshal := json.Unmarshal(value,&data_msg)
		if err_unmarshal != nil{
			log.Errorf(ERR_UNMARSHAL_INVALID+"%s\n",err_unmarshal)
			return getRetMsg(RET_CODE_ARGS_UNMARSHAL_ERR,ERR_UNMARSHAL_INVALID+err_unmarshal.Error()),errors.New(ERR_UNMARSHAL_INVALID+err_unmarshal.Error())
		}

		data_array = append(data_array,data_msg)
	}
	return data_array,nil
}
// Query callback representing the query of a chaincode
func (t *SimpleChaincode) Query(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
	if len(args) != 1{
		log.Errorf(ERR_ARGS_LENGTH_INVALID)
		return getRetMsg(RET_CODE_ARGS_LENGTH_ERR,ERR_ARGS_LENGTH_INVALID), nil
		//return getRetMsg(RET_CODE_ARGS_LENGTH_ERR,ERR_ARGS_LENGTH_INVALID),errors.New(ERR_ARGS_LENGTH_INVALID)
	}

	switch function {
	case "check_transfer":
		tran_data := msg_transfer{}
                err_unmarshal:=json.Unmarshal([]byte(args[0]),&tran_data)
                if err_unmarshal != nil{
                        log.Errorf(ERR_UNMARSHAL_INVALID+"%s\n",err_unmarshal)
                        return getRetMsg(RET_CODE_ARGS_UNMARSHAL_ERR,ERR_UNMARSHAL_INVALID),nil
                        //return getRetMsg(RET_CODE_ARGS_UNMARSHAL_ERR,ERR_UNMARSHAL_INVALID),errors.New(ERR_UNMARSHAL_INVALID)
                }
                log.Infof("transfer %s",tran_data)

		msgValid,err:=checkTransferMsg(stub,&tran_data)
        	if err != nil{
               		return msgValid, nil
               		//return msgValid,err
        	}
		return getRetMsg(RET_CODE_OK,"check ok"),nil
	case "query_details":
		//data_array := make([]msg_transfer, 0)
		data_query_order_id := msg_details{}
		err_unmarshal := json.Unmarshal([]byte(args[0]), &data_query_order_id)
		if err_unmarshal != nil {
			log.Errorf(ERR_UNMARSHAL_INVALID + "%s\n", err_unmarshal)
			return getRetMsg(RET_CODE_ARGS_UNMARSHAL_ERR, ERR_UNMARSHAL_INVALID + err_unmarshal.Error()), nil
			//return getRetMsg(RET_CODE_ARGS_UNMARSHAL_ERR, ERR_UNMARSHAL_INVALID + err_unmarshal.Error()), errors.New(ERR_UNMARSHAL_INVALID + err_unmarshal.Error())
		}
		data_array, err := queryIter(stub,data_query_order_id.Mer_id + "0",data_query_order_id.Mer_id + "1" )

		if err != nil {
			return data_array.([]byte), err
		}
//		data_array = append(data_array,part_data_array.([]msg_transfer))

		data_byte_array,err_data_array_marshal:=json.Marshal(data_array)
		if err_data_array_marshal != nil{
			log.Errorf(ERR_MARSHAL_INVALID+"%s\n",err_data_array_marshal)
			return getRetMsg(RET_CODE_DATA_MARSHAL_ERR,ERR_MARSHAL_INVALID+err_data_array_marshal.Error()),nil
			//return getRetMsg(RET_CODE_DATA_MARSHAL_ERR,ERR_MARSHAL_INVALID+err_data_array_marshal.Error()),errors.New(ERR_MARSHAL_INVALID+err_data_array_marshal.Error())
		}

		return getRetMsg(RET_CODE_OK,string(data_byte_array)),nil
	case "query_by_mer_order_id":
//		data_array := make([]msg_transfer, 0)
		data_query_order_id := msg_query_order_id{}
		err_unmarshal := json.Unmarshal([]byte(args[0]), &data_query_order_id)
		if err_unmarshal != nil {
			log.Errorf(ERR_UNMARSHAL_INVALID + "%s\n", err_unmarshal)
			return getRetMsg(RET_CODE_ARGS_UNMARSHAL_ERR, ERR_UNMARSHAL_INVALID + err_unmarshal.Error()),nil
			//return getRetMsg(RET_CODE_ARGS_UNMARSHAL_ERR, ERR_UNMARSHAL_INVALID + err_unmarshal.Error()), errors.New(ERR_UNMARSHAL_INVALID + err_unmarshal.Error())
		}
		data_array, err := queryIter(stub,data_query_order_id.Mer_id + "1" + data_query_order_id.Begin_order_id+"0", data_query_order_id.Mer_id + "1" + data_query_order_id.End_order_id+":")

		if err != nil {
			return data_array.([]byte), err
		}
//		data_array = append(data_array,part_data_array.([]msg_transfer))

		data_byte_array,err_data_array_marshal:=json.Marshal(data_array)
		if err_data_array_marshal != nil{
			log.Errorf(ERR_MARSHAL_INVALID+"%s\n",err_data_array_marshal)
			return getRetMsg(RET_CODE_DATA_MARSHAL_ERR,ERR_MARSHAL_INVALID+err_data_array_marshal.Error()),nil
			//return getRetMsg(RET_CODE_DATA_MARSHAL_ERR,ERR_MARSHAL_INVALID+err_data_array_marshal.Error()),errors.New(ERR_MARSHAL_INVALID+err_data_array_marshal.Error())
		}

		return getRetMsg(RET_CODE_OK,string(data_byte_array)),nil
	case "query_by_mer_date":
		//data_array := make([]msg_transfer, 0)
		data_query_date := msg_query_date{}
		err_unmarshal := json.Unmarshal([]byte(args[0]), &data_query_date)
		if err_unmarshal != nil {
			log.Errorf(ERR_UNMARSHAL_INVALID + "%s\n", err_unmarshal)
			return getRetMsg(RET_CODE_ARGS_UNMARSHAL_ERR, ERR_UNMARSHAL_INVALID + err_unmarshal.Error()),nil
			//return getRetMsg(RET_CODE_ARGS_UNMARSHAL_ERR, ERR_UNMARSHAL_INVALID + err_unmarshal.Error()), errors.New(ERR_UNMARSHAL_INVALID + err_unmarshal.Error())
		}
		part_data_array, err := queryIter(stub,data_query_date.Mer_id + "1" + data_query_date.Begin_mer_date, data_query_date.Mer_id + "1" + data_query_date.End_mer_date)

		if err != nil {
			return part_data_array.([]byte), err
		}
		type pay_and_payed struct {
        		Pay []msg_transfer
        		Payed []msg_transfer
		}
	
		mer_id_list := pay_and_payed{}
		data_array := make([]msg_transfer, 0)
		for i:=0; i< len(part_data_array.([]msg_transfer));i++{
			if part_data_array.([]msg_transfer)[i].Transfer_type != "upload"{ 
				data_array = append(data_array,part_data_array.([]msg_transfer)[i])
			}
              	}

		mer_id_list.Pay = data_array
//		for i:=0; i< len(part_data_array.([]msg_transfer));i++{
//			data_array = append(data_array,part_data_array.([]msg_transfer)[i])
//		}

		part_data_array, err = queryIter(stub,data_query_date.Mer_id+"0"+data_query_date.Begin_mer_date,data_query_date.Mer_id+"0"+data_query_date.End_mer_date)
//		data_array = append(data_array,part_data_array.([]msg_transfer))

		mer_id_list.Payed = part_data_array.([]msg_transfer)
		
//		for j:=0; j< len(part_data_array.([]msg_transfer));j++{
//			data_array = append(data_array,part_data_array.([]msg_transfer)[j])
//		}

		data_byte_array,err_data_array_marshal:=json.Marshal(mer_id_list)

		if err_data_array_marshal != nil{
			log.Errorf(ERR_MARSHAL_INVALID+"%s\n",err_data_array_marshal)
			return getRetMsg(RET_CODE_DATA_MARSHAL_ERR,ERR_MARSHAL_INVALID+err_data_array_marshal.Error()),nil
			//return getRetMsg(RET_CODE_DATA_MARSHAL_ERR,ERR_MARSHAL_INVALID+err_data_array_marshal.Error()),errors.New(ERR_MARSHAL_INVALID+err_data_array_marshal.Error())
		}

		return getRetMsg(RET_CODE_OK,string(data_byte_array)),nil
	case "balance":
		balance_data := msg_balance{}
		err_unmarshal:=json.Unmarshal([]byte(args[0]),&balance_data)
		if err_unmarshal != nil{
			log.Errorf(ERR_UNMARSHAL_INVALID+"%s\n",err_unmarshal)
			return getRetMsg(RET_CODE_ARGS_UNMARSHAL_ERR,ERR_UNMARSHAL_INVALID),nil
			//return getRetMsg(RET_CODE_ARGS_UNMARSHAL_ERR,ERR_UNMARSHAL_INVALID),errors.New(ERR_UNMARSHAL_INVALID)
		}
		mer_id_exist,err := checkMerExist(stub,balance_data.Mer_id)
		if err != nil{
			return mer_id_exist,nil
		}
		balance_value,err_balance:=getBalance(stub,balance_data.Mer_id)
		if err_balance != nil{
			return balance_value,nil
		}
		return getRetMsg(RET_CODE_OK,string(balance_value)),nil
	default:
		log.Errorf(ERR_QUERY_TYPE)
		return getRetMsg(RET_CODE_QUERY_TYPE_ERR,ERR_QUERY_TYPE), nil
		//return getRetMsg(RET_CODE_QUERY_TYPE_ERR,ERR_QUERY_TYPE),errors.New(ERR_QUERY_TYPE)
	}



	return nil,nil
}

func main() {
	err := shim.Start(new(SimpleChaincode))
	if err != nil {
		fmt.Printf("Error starting Simple chaincode: %s", err)
	}
}

