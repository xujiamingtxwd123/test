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
)

var log, _ = logging.GetLogger("txs_batch_record")

type SimpleChaincode struct {
}

// Init takes two arguments, a string and int. The string will be a key with
// the int as a value.
func (t *SimpleChaincode) Init(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
	return nil,nil
}

type msg_txs_head struct{
	Mer_id string
	Order_id string
	Mer_date string
}

type msg_txs_batch struct{
	Mer_id string
	Order_id string
	Mer_date string
	Pay_amount string
	Tx_batch interface{}
}
type msg_txs_result struct{
	Mer_id string
	Order_id string
	Mer_date string
	Tx_result []interface{}
}
const(
	
	ERR_ARGS_LENGTH_INVALID = "args length err "
	ERR_UNMARSHAL_INVALID = "unmarshal err "	
	ERR_MARSHAL_INVALID = "marshal err "
	ERR_GET_STATE="get state err "
	ERR_INVOKE_TYPE="invoke type err "
	ERR_RANGE_QUERY_STATE="range query state err "
	ERR_ITER_NEXT = "iter next err "
	ERR_QUERY_TYPE = "query type err"
	ERR_TX_MSG_EXIST = "tx msgs already exist"

	RET_CODE_OK="0000"
	RET_CODE_ARGS_LENGTH_ERR="0001"
	RET_CODE_ARGS_UNMARSHAL_ERR="0002"
	RET_CODE_TXS_MARSHAL_ERR="0003"
	RET_CODE_GET_STATE_ERR="0004"
	RET_CODE_DATA_ALREADY_EXIST="0005"
	RET_CODE_RANGE_QUERY_STATE="0006"
	RET_CODE_ITER_NEXT_ERR="0007"
	RET_CODE_QUERY_TYPE_ERR="0008"
	RET_CODE_INVOKE_TYPE_ERR="0009"
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
// Invoke queries another chaincode and updates its own state
func (t *SimpleChaincode) Invoke(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
	if function == "txs_record" || function == "txs_result_record"{
		if len(args) != 1{
			log.Errorf(ERR_ARGS_LENGTH_INVALID)
			return getRetMsg(RET_CODE_ARGS_LENGTH_ERR,ERR_ARGS_LENGTH_INVALID),errors.New(ERR_ARGS_LENGTH_INVALID)
		}
		
		var msg_txs_data interface{}
		var head string
		var txs_msg msg_txs_head
		var err_unmarshal error
		if function == "txs_record"{
			head = ""
			temp_data := msg_txs_batch{}
			err_unmarshal = json.Unmarshal([]byte(args[0]),&temp_data)
			txs_msg.Mer_id = temp_data.Mer_id
			txs_msg.Order_id = temp_data.Order_id
			txs_msg.Mer_date = temp_data.Mer_date
			msg_txs_data = temp_data

		}else{
			head = "result"
			temp_data := msg_txs_result{}
			err_unmarshal = json.Unmarshal([]byte(args[0]),&temp_data)	
			txs_msg.Mer_id = temp_data.Mer_id
			txs_msg.Order_id = temp_data.Order_id
			txs_msg.Mer_date = temp_data.Mer_date
			msg_txs_data = temp_data
		}

		if err_unmarshal != nil{
			log.Errorf(ERR_UNMARSHAL_INVALID+"%s\n",err_unmarshal)
			return getRetMsg(RET_CODE_ARGS_UNMARSHAL_ERR,ERR_UNMARSHAL_INVALID),errors.New(ERR_UNMARSHAL_INVALID)
		}
		log.Infof("txs_msg:%x",txs_msg.Order_id)
		key_with_date := (head+txs_msg.Mer_id + "1" + txs_msg.Mer_date +"1"+ txs_msg.Order_id)
		key_without_date:=(head+txs_msg.Mer_id + "0" + txs_msg.Order_id+"0")

		value_with_date,err_get_state_t1 := stub.GetState(key_with_date) 
	
		if err_get_state_t1 != nil{
			log.Errorf(ERR_GET_STATE+"%s\n",err_get_state_t1)
			return getRetMsg(RET_CODE_GET_STATE_ERR,ERR_GET_STATE+err_get_state_t1.Error()),errors.New(ERR_GET_STATE+err_get_state_t1.Error())
		}
		
		if value_with_date != nil{
			log.Errorf("mer_id=%s, mer_order_id=%s,mer_date=%s,already exists",txs_msg.Mer_id,txs_msg.Order_id,txs_msg.Mer_date)
			return getRetMsg(RET_CODE_DATA_ALREADY_EXIST,ERR_TX_MSG_EXIST),errors.New(ERR_TX_MSG_EXIST)
		}

		value_without_date,err_get_state_t2:= stub.GetState(key_without_date) 
		if err_get_state_t2!= nil{
			log.Errorf(ERR_GET_STATE+"%s\n",err_get_state_t2)
			 return getRetMsg(RET_CODE_GET_STATE_ERR,ERR_GET_STATE+err_get_state_t2.Error()),errors.New(ERR_GET_STATE+err_get_state_t2.Error())                    
		}
		if value_without_date != nil{
			log.Errorf("mer_id=%s, mer_order_id=%s,mer_date=%s,already exists",txs_msg.Mer_id,txs_msg.Order_id,txs_msg.Mer_date)
                        return getRetMsg(RET_CODE_DATA_ALREADY_EXIST,ERR_TX_MSG_EXIST),errors.New(ERR_TX_MSG_EXIST)		
		}
				
		txs_byte_array,err_marshal:=json.Marshal(msg_txs_data)
		log.Infof("txs_byte_array:%x",txs_byte_array)
		if err_marshal != nil{
   			log.Errorf(ERR_MARSHAL_INVALID+"%s\n",err_marshal)
                        return getRetMsg(RET_CODE_TXS_MARSHAL_ERR,ERR_MARSHAL_INVALID+err_marshal.Error()),errors.New(ERR_MARSHAL_INVALID+err_marshal.Error())
		}
		stub.PutState(key_with_date,txs_byte_array)
		stub.PutState(key_without_date,txs_byte_array)
		if head == ""{
			log.Infof("Set Event\n")	
			stub.SetEvent("txs_batch_marshal",txs_byte_array)
		}
	}else{
		log.Errorf(ERR_INVOKE_TYPE)
		return getRetMsg(RET_CODE_INVOKE_TYPE_ERR,ERR_INVOKE_TYPE),errors.New(ERR_INVOKE_TYPE)
	}
	return nil,nil
}

type query_date struct{
	Mer_id string
	Begin_mer_date string
	End_mer_date string
}

type query_order struct{
	Mer_id string
	Begin_order_id string
	End_order_id string
	
}
// Query callback representing the query of a chaincode
func (t *SimpleChaincode) Query(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
		if len(args) != 1{
			log.Errorf(ERR_ARGS_LENGTH_INVALID)
			return getRetMsg(RET_CODE_ARGS_LENGTH_ERR,ERR_ARGS_LENGTH_INVALID),errors.New(ERR_ARGS_LENGTH_INVALID)
		}

		var head string
		var iter shim.StateRangeQueryIteratorInterface	
	if function == "query_by_mer_date" || function == "result_query_by_mer_date"{
		tx_query_date := query_date{}
		err_unmarshal:=json.Unmarshal([]byte(args[0]),&tx_query_date)
		if err_unmarshal != nil{
			log.Errorf(ERR_UNMARSHAL_INVALID+"%s\n",err_unmarshal)
                        return getRetMsg(RET_CODE_ARGS_UNMARSHAL_ERR,ERR_UNMARSHAL_INVALID+err_unmarshal.Error()),errors.New(ERR_UNMARSHAL_INVALID+err_unmarshal.Error())
		}
		var err error
		if function == "result_query_by_mer_date"{
			head="result"
		}else{
			head=""
		}
		iter,err =stub.RangeQueryState(head+tx_query_date.Mer_id+"1"+tx_query_date.Begin_mer_date+"0",head+tx_query_date.Mer_id+"1"+tx_query_date.End_mer_date+":")
			
		if err != nil{
			log.Errorf(ERR_RANGE_QUERY_STATE+"%s\n",err)
                	return getRetMsg(RET_CODE_RANGE_QUERY_STATE,ERR_RANGE_QUERY_STATE+err.Error()), errors.New(ERR_RANGE_QUERY_STATE+err.Error())
        	}
	}else if function == "query_by_order_id" || function == "result_query_by_order_id"{
		tx_query_order := query_order{}
		err_unmarshal:=json.Unmarshal([]byte(args[0]),&tx_query_order)
                if err_unmarshal != nil{
			log.Errorf(ERR_UNMARSHAL_INVALID+"%s\n",err_unmarshal)
                        return getRetMsg(RET_CODE_ARGS_UNMARSHAL_ERR,ERR_UNMARSHAL_INVALID+err_unmarshal.Error()),errors.New(ERR_UNMARSHAL_INVALID+err_unmarshal.Error())

                }
		var err error
		if function == "result_query_by_order_id"{
			head="result"
		}else{
			head=""
		}
                iter,err =stub.RangeQueryState(head + tx_query_order.Mer_id+"0"+tx_query_order.Begin_order_id+"0",head + tx_query_order.Mer_id + "0"+tx_query_order.End_order_id+":")
		if err != nil{
			log.Errorf(ERR_RANGE_QUERY_STATE+"%s\n",err)
                	return getRetMsg(RET_CODE_RANGE_QUERY_STATE,ERR_RANGE_QUERY_STATE+err.Error()), errors.New(ERR_RANGE_QUERY_STATE+err.Error())
        	}
	}else{
		log.Errorf(ERR_QUERY_TYPE)
		return getRetMsg(RET_CODE_QUERY_TYPE_ERR,ERR_QUERY_TYPE),errors.New(ERR_QUERY_TYPE)
	}
	

        defer iter.Close()
	
	tx_array:=make([]interface{},0)		
        var txs_msg interface{}
	var err_unmarshal error

	for iter.HasNext(){
                _,value,err:=iter.Next()
		if err != nil{
      			log.Errorf(ERR_ITER_NEXT+"%s\n",err)	
                        return getRetMsg(RET_CODE_ITER_NEXT_ERR,ERR_ITER_NEXT + err.Error()),errors.New(ERR_ITER_NEXT + err.Error())
                }
		if head == ""{
			 temp_data := msg_txs_batch{}
			 err_unmarshal = json.Unmarshal(value,&temp_data)
			 txs_msg = temp_data
		}else{	
			 temp_data := msg_txs_result{}
			 err_unmarshal = json.Unmarshal(value,&temp_data)
			 txs_msg = temp_data
		}
                if err_unmarshal != nil{
       			log.Errorf(ERR_UNMARSHAL_INVALID+"%s\n",err_unmarshal)
                        return getRetMsg(RET_CODE_ARGS_UNMARSHAL_ERR,ERR_UNMARSHAL_INVALID+err_unmarshal.Error()),errors.New(ERR_UNMARSHAL_INVALID+err_unmarshal.Error())
                }

                tx_array = append(tx_array,txs_msg)
         }
         txs_byte_array,err_tx_array_marshal:=json.Marshal(tx_array)
	if err_tx_array_marshal != nil{
		log.Errorf(ERR_MARSHAL_INVALID+"%s\n",err_tx_array_marshal)
                return getRetMsg(RET_CODE_TXS_MARSHAL_ERR,ERR_MARSHAL_INVALID+err_tx_array_marshal.Error()),errors.New(ERR_MARSHAL_INVALID+err_tx_array_marshal.Error())
        }

        return getRetMsg(RET_CODE_OK,string(txs_byte_array)),nil
}

func main() {
	err := shim.Start(new(SimpleChaincode))
	if err != nil {
		fmt.Printf("Error starting Simple chaincode: %s", err)
	}
}
