#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string &relation,
                       const int attrCnt,
                       const attrInfo attrList[]) {
    // Insert a tuple with the given attribute values (in attrList) in relation.
    // The value of the attribute is supplied in the attrValue member of the attrInfo structure.
    // Since the order of the attributes in attrList[] may not be the same as in the relation,
    // you might have to rearrange them before insertion.
    // If no value is specified for an attribute, you should reject the insertion as Minirel does not implement NULLs.

    Status status;
    RID outRid;

    //Check for NULLs in attrList
    for (int k = 0; k < attrCnt; k++) {
        if (attrList[k].attrValue != NULL) continue;
        cout<<"missing attribute: "<< attrList[k].attrName << endl;
        return ATTRNOTFOUND;
    }

    // get relation descriptor for the relation
    RelDesc relationDesc;
    status = relCat->getInfo(relation, relationDesc);
    if (status != OK) return status;

    // get attribute descriptor & attribute count
    int allAttrCnt;
    AttrDesc *attrDesc;
    string relationName = relationDesc.relName;
    status = attrCat->getRelInfo(relationName, allAttrCnt, attrDesc);
    if (status != OK) return status;

    //get the total record length
    int recLen = 0;
    for (int i = 0; i < attrCnt; i++) {
        recLen += attrDesc[i].attrLen;
    }

    // allocate space for the record
    Record rec;
    rec.length = recLen;
    char outData[rec.length];
    rec.data = &outData;


    //rearrange attribute list to match order of attributes in relation
    int recordOffset = 0;
    for (int i = 0; i < attrCnt; i++) {
        AttrDesc currAttr = attrDesc[i];
        bool found = false;
        for (int j = 0; j < attrCnt; j++) {
            attrInfo currInfo = attrList[j];
            int flag = strcmp(currAttr.attrName, currInfo.attrName);
            if (0 != flag) continue;

            // pointer for memcpy()
            void *srcPointer;

            // handle different types, convert ints and floats
            int targetType = currAttr.attrType;

            // attribute is string type
            if (targetType == 0){
                srcPointer = currInfo.attrValue;
                found = true;
            }

            // attribute is integer type
            if (targetType == 1){
                int buffer;
                buffer = atoi((char *) currInfo.attrValue);
                srcPointer = &buffer;
                found = true;
            }

            // attribute is float type
            if (targetType == 2){
                float buffer;
                buffer = atof((char *) currInfo.attrValue);
                srcPointer = &buffer;
                found = true;
            }

            memcpy((char *) outData + recordOffset, (char *) srcPointer, currAttr.attrLen);
            recordOffset += currAttr.attrLen;
        }

        // not found in this loop for currAttr.attrName
        if (!found){
            cout<<"not found attribute for: "<< currAttr.attrName << endl;
            return ATTRNOTFOUND;
        }
    }

    // create InsertFileScan
    InsertFileScan insertFileScan(relation, status);
    // insert the record
    status = insertFileScan.insertRecord(rec, outRid);
    return status;
}



