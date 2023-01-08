#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{
// part 6
Status status = OK;
// check if the input attributes are valid
if (relation.empty()) {
    status = BADCATPARM; // bad catalog parameter
    return status;
}

RID rid; // record id
AttrDesc attributeDesc;
HeapFileScan *heapFileScan = new HeapFileScan(relation, status);
if (status!=OK){return status;}

// preprocess the attribute value for later scanning
const char* filter;
int val1;
float val2;
if (type == INTEGER){
    val1 = atoi(attrValue);
    filter = (char*)&val1;
}
else if(type == FLOAT){
    val2 = atof(attrValue);
    filter = (char*)&val2;
}
else if(type == STRING){
    filter = attrValue;
}

// initial scan
if (attrName == ""){
    status = heapFileScan->startScan(0, 0, type, NULL, op);
}else{
    // check catelog
    status = attrCat->getInfo(relation, attrName, attributeDesc);
    if (status!=OK){ return status; }
    status = heapFileScan->startScan(attributeDesc.attrOffset, attributeDesc.attrLen, type, filter, op);
}
if (status!=OK){
    cout << "fail to startScan" << endl;
    return status;
}

// look up the target record and delete
//Record record; // for debugging
while(status == OK){
    status = heapFileScan->scanNext(rid);
    if (status != OK){ break;} // exit when get to the end of file
    heapFileScan->deleteRecord();
}

delete heapFileScan; // clean up
return OK;

}


