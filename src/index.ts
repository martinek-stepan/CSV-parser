import { Transform, TransformCallback, TransformOptions } from 'stream';


interface IOptions
{
    // User can define specific header or indicate if first row should be used as header by setting this property to true
    headers?: Array<string> | boolean;

    // Used for override of original escape character (")
    escape?: string;
    
    // Used for override of original row break characters (CRLF)
    rowBreak?: string;
    
    // Used for override of original column break character (,)
    columnBreak?: string;

    // When true, throw exception when number of columns doesnt match header / first row
    strict?: boolean;
}

const defaultOptions: IOptions = {
    headers: true,
    escape: '"',
    rowBreak: "\r\n",
    columnBreak: ",",
    strict: true
}

interface ICSVRecord
{
    [key: string]: string
}

class CSVParse extends Transform
{
    private options: IOptions;
    private leftoverData?: string;
    private headers: Array<string> = [];
    private firstRowParsed = false;

    constructor(parserOptions?: IOptions, streamOptions?: TransformOptions) {
        super(streamOptions);

        this.options = { ...defaultOptions, ...parserOptions };

        // Check if escape string is not present in break strings, throw exception if it is      
        this.checkOptions();
    }

    // Leftover data always starts from begining of row
    // Find row ending or check if we are in flush
    // Process row

    // Transform function, called when data are available
    public _transform(chunk: any, encoding: BufferEncoding, callback: TransformCallback): void
    {
        // Type of data we got should be string
        let data = String(chunk);
        // If we have leftover data from last chunk append new chunk to them
        if (this.leftoverData) {
          data = this.leftoverData + data;
          this.leftoverData = "";
        }

        this.processData(data, callback, false);
    }

    // Transform function, called after all data was delivered (at end of stream)
    public _flush(callback: TransformCallback): void
    {         
        if (this.leftoverData)
        {
            this.processData(this.leftoverData, callback, true);
        }
    }

    // Check if escape string is not present in break strings, throw exception if it is   
    private checkOptions(): void
    {
        if (this.options.columnBreak!.includes(this.options.escape!))
        {
            throw new Error("Column break ["+this.options.columnBreak!+"] can not include, escape string ["+this.options.escape!+"]!");
        }
        if (this.options.columnBreak!.includes(this.options.escape!))
        {
            throw new Error("Row break ["+this.options.rowBreak!+"] can not include, escape string ["+this.options.escape!+"]!");
        }
    }

    private processColumns(colums: Array<string>, callback: TransformCallback): void
    {
        if (this.options.headers === true && this.firstRowParsed === false)
        {
            this.firstRowParsed = true;
            this.headers = colums;
            return;
        }

        // Number of colums we parsed and number of headers we have doesnt match
        if (this.headers.length !== colums.length)
        {
            // If strict is enabled we return error instead of data
            if (this.options.strict === true)
            {
                callback(new Error("Number of colums("+colums.length+") does not match number of headers("+this.headers.length+")!"), null);
                return;
            }
            // Otherwise we add new header names (number or placeholder text)
            else if (colums.length > this.headers.length)
            {
                for (let i = this.headers.length; i < colums.length; i++)
                {
                    if (this.options.headers !== false)
                    {
                        let name = "___UNKNONW_HEADER_"+i+"___";
                        // In unlikely case this name already exists append _ till we have a unique one
                        while (this.headers.includes(name))
                        {
                            name += "_";
                        }
                        this.headers.push(name);
                    }
                    else
                    {
                        this.headers.push(i.toString());
                    }
                }
            }
        }

        const obj: ICSVRecord = {};
        for (let i = 0; i < colums.length; i++)
        {
            obj[this.headers[i]] = colums[i];
        }

        callback(null, obj);
    }

    private processRow(data: string, rowBegin:number, rowEnd:number, callback: TransformCallback): void
    {
        let columnBegin = rowBegin;
        // The nested function findNextEscaping can throw exception, but said exception would already be thrown and caught earlier in executed code (processData:findNextRI)
        let columnEnd = this.findNextCI(data, columnBegin);
        const columns: Array<string> = [];

        while(columnEnd !== -1)
        {
            columns.push(data.substring(columnBegin + this.options.columnBreak!.length, columnEnd));

            columnBegin = columnEnd;
            // The nested function findNextEscaping can throw exception, but said exception would already be thrown and caught earlier in executed code (processData:findNextRI)
            columnEnd = this.findNextCI(data, columnBegin);
        }
        
        columns.push(data.substring(columnBegin + this.options.columnBreak!.length, columnEnd));

        this.processColumns(columns, callback);
    }

    private processData(data: string, callback: TransformCallback, endOfData: boolean): void
    {
        let rowBegin = 0;
        let rowEnd = -1;
        try
        {
            rowEnd = this.findNextRI(data, rowBegin);
        }
        // We did not find end of escaping sequence - more data is needed
        catch(err)
        {
            this.leftoverData = data;
            return;    
        }

        while(rowEnd !== -1)
        {
            this.processRow(data, rowBegin, rowEnd, callback); 

            rowBegin = rowEnd;
            
            try
            {
                this.findNextRI(data, rowBegin);
            }
            // We did not find end of escaping sequence - more data is needed
            catch(err)
            {
                this.leftoverData = data.substr(rowBegin + this.options.rowBreak!.length);
                return;    
            }
        }
        
        // In case this was called from flush, we will say end of row was end of data
        if (endOfData === true)
        {
            this.processRow(data, rowBegin, data.length - 1, callback); 
        }
        // Othervise save leftover data for next call
        else
        {
            this.leftoverData = data.substr(rowBegin + this.options.rowBreak!.length);
        }
    }

    private findNextRI(data:string, startIndex: number): number{
        return this.findNext(data, startIndex, this.options.rowBreak!);
    }

    private findNextCI(data:string, startIndex: number): number{
        return this.findNext(data, startIndex, this.options.columnBreak!);        
    }

    private findNext(data: string, startIndex: number, delimiter: string): number {

        let index = data.indexOf(delimiter, startIndex);
        const escapeTuple = this.findNextEscaping(data, startIndex);

        if (escapeTuple !== null && index >= escapeTuple[0] && index <= escapeTuple[1])
        {
            index = data.indexOf(delimiter, escapeTuple[1] + this.options.escape!.length);
        }

        return index;
    }

    private findNextEscaping(data: string, startIndex: number) : [number, number] | null
    {
        let doubleEscaped = data.indexOf(this.options.escape!+this.options.escape!, startIndex);
        let escaped = data.indexOf(this.options.escape!, startIndex);

        // If we find escaped on same index as double escaped we continue looking (until we cant find either)
        while (escaped === doubleEscaped && escaped !== -1 && doubleEscaped !== -1)
        {
            doubleEscaped = data.indexOf(this.options.escape!+this.options.escape!, doubleEscaped + this.options.escape!.length*2);
            escaped = data.indexOf(this.options.escape!, escaped+this.options.escape!.length*2);
        }

        if (escaped !== -1)
        {
            doubleEscaped = data.indexOf(this.options.escape!+this.options.escape!, escaped+this.options.escape!.length);
            let escapedEnd = data.indexOf(this.options.escape!, escaped+this.options.escape!.length);
    
            // If we find escaped on same index as double escaped we continue looking (until we cant find either)
            while (escapedEnd === doubleEscaped && escapedEnd !== -1 && doubleEscaped !== -1)
            {
                doubleEscaped = data.indexOf(this.options.escape!+this.options.escape!, doubleEscaped+this.options.escape!.length*2);
                escapedEnd = data.indexOf(this.options.escape!, escapedEnd+this.options.escape!.length*2);
            }

            if (escapedEnd === -1)
            {
                throw new Error("Not enough data to finish escaping");
            }

            return [escaped, escapedEnd];
        }

        return null;

    }

}

export default CSVParse;