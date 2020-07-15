import {ThunkAction, ThunkDispatch} from "../reduxTypes"
import HttpService from "../../http/HttpService"
import {NodeValidationError, PropertiesType, VariableTypes, NodeType, UIParameter} from "../../types"

import {debounce} from "lodash"

export type NodeValidationUpdated = { type: "NODE_VALIDATION_UPDATED", validationData: ValidationData}
export type NodeDetailsActions = NodeValidationUpdated

export type ValidationData = {
    parameters? : Record<string, UIParameter>,
    validationErrors: NodeValidationError[],
    validationPerformed: boolean,
}

type ValidationRequest = {
    nodeData: NodeType,
    variableTypes: VariableTypes,
    processProperties: PropertiesType,
}

function nodeValidationDataUpdated(validationData: ValidationData): NodeValidationUpdated {
  return {type: "NODE_VALIDATION_UPDATED", validationData: validationData}
}

//we don't return ThunkAction here as it would not work correctly with debounce
function validate(processId: string, request: ValidationRequest, dispatch: ThunkDispatch) {
  HttpService.validateNode(processId, request).then(data => dispatch(nodeValidationDataUpdated(data.data)))
}

//TODO: use sth better, how long should be timeout?
const debouncedValidate = debounce(validate, 500)

export function updateNodeData(processId: string, variableTypes: VariableTypes, nodeData: NodeType, processProperties: PropertiesType): ThunkAction {
  //groups and Properties are "special types" which are not compatible with NodeData in BE
  if (nodeData.type && nodeData.type !== "_group" && nodeData.type !== "Properties") {
    return (dispatch) => debouncedValidate(processId, {
      nodeData, variableTypes, processProperties}, dispatch)
  } else {
    return () => {/* ignore invocation */}
  }

}
 
