#include "catalog.h"
#include "query.h"


// forward declaration
const Status ScanSelect(const string & result,
			const int projCnt,
			const AttrDesc projNames[],
			const AttrDesc *attrDesc,
			const Operator op,
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result,
		       const int projCnt,
		       const attrInfo projNames[],
		       const attrInfo *attr,
		       const Operator op,
		       const char *attrValue)
{
   // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;
    Status status;

    // go through the projection list and look up each in the attr cat to
    // get an AttrDesc structure (for offset, length, etc)
    AttrDesc projDesc[projCnt];
    for (int i = 0; i < projCnt; i++)
    {
        Status status = attrCat->getInfo(projNames[i].relName,
                                         projNames[i].attrName,
                                         projDesc[i]);
        if (status != OK)
        {
            return status;
        }
    }

    // check the attr is not null
    // get AttrDesc structure for the attribute
    AttrDesc attrDesc;
    if(attr != NULL) {
        status = attrCat->getInfo(attr->relName,
                                  attr->attrName,
                                  attrDesc);
        if (status != OK)
        {
            return status;
        }
    }

    // get output record length from attrdesc structures
    int reclen = 0;
    for (int i = 0; i < projCnt; i++)
    {
        reclen += projDesc[i].attrLen;
    }

    // covert attrValue to proper type
    const char *filter;
    switch (attrDesc.attrType) {
        case INTEGER: {
            int value = atoi(attrValue);
            filter = (char *)&value;
            break;
        }
        case FLOAT: {
            float value = atof(attrValue);
            filter = (char *)&value;
            break;
        }
        default:
            filter = attrValue;
    }
    // non-conditional search
    if(attr == NULL) {
        return ScanSelect(result, projCnt, projDesc, &projDesc[0], op, NULL, reclen);
    }
    return ScanSelect(result, projCnt, projDesc, &attrDesc, op, filter, reclen);
}


const Status ScanSelect(const string & result,
#include "stdio.h"
#include "stdlib.h"
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;
    Status status;

    // open the result table
    InsertFileScan resultRel(result, status);
    if (status != OK) { return status; }

    // start scan the table
    HeapFileScan scan(string(attrDesc->relName), status);
    scan.startScan(attrDesc->attrOffset,
                       attrDesc->attrLen,
                       (Datatype) attrDesc->attrType,
                       filter,
                       op);
    if (status != OK) { return status; }

    // create output
    char outputData[reclen];
    Record outputRec;
    Record rec;
    outputRec.data = (void *) outputData;
    outputRec.length = reclen;

    // scan and get all record
    RID rid;
    while(scan.scanNext(rid) == OK) {
        status = scan.getRecord(rec);
        //ASSERT(status == OK);

        // we have a match, copy data into the output record
        int offset = 0;
        for (int i = 0; i < projCnt; i++)
        {
            memcpy(outputData + offset,
                   (char *) rec.data + projNames[i].attrOffset,
                   projNames[i].attrLen);
            offset += projNames[i].attrLen;
        } // end copy attrs

        // add the new record to the output relation
        RID outRID;
        status = resultRel.insertRecord(outputRec, outRID);
        //ASSERT(status == OK);
    }
    return status;
}
