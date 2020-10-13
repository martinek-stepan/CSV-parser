/**
 * @author Stepan Martinek <ste.martinek+gh@gmail.com>
 */

import { Transform, TransformCallback, TransformOptions } from 'stream';


// External structure with optional properties used for overriding default config
export interface ICSVParserOptions
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

// Internal structure that doesnt contain optionals, since we are using default config
interface ICSVParserOptions_
{
    // User can define specific header or indicate if first row should be used as header by setting this property to true
    headers: Array<string> | boolean;

    // Used for override of original escape character (")
    escape: string;
    
    // Used for override of original row break characters (CRLF)
    rowBreak: string;
    
    // Used for override of original column break character (,)
    columnBreak: string;

    // When true, throw exception when number of columns doesnt match header / first row
    strict: boolean;
}

const defaultOptions: ICSVParserOptions_ = {
    headers: true,
    escape: '"',
    rowBreak: '\r\n',
    columnBreak: ',',
    strict: true
};

// Helper inferface to compose dynamic key-value object
export interface ICSVRecord
{
    [key: string]: string
}

// Helper inferface for keeping data about start of field
interface IDelimiterObject
{
    start: number,
    escaped: boolean
}


export class ColumnBreakError extends Error {
    constructor(columnBreak: string, escape: string) {
        super('Column break [' + columnBreak + '] can not include, escape string [' + escape + ']!');
    }
}
export class RowBreakError extends Error {
    constructor(rowBreak: string, escape: string) {
        super('Row break [' + rowBreak + '] can not include, escape string [' + escape + ']!');
    }
}

export class ColumnMissmatchError extends Error {
    constructor(columnLenght: number, headersLength: number) {
        super('Number of colums(' + columnLenght + ') does not match number of headers(' + headersLength + ')!');
    }
}


export class CSVParser extends Transform
{
    private options: ICSVParserOptions_;
    private headers: Array<string> = [];

    private firstRowParsed = false;

    private leftoverData = '';
    private leftoverColumns: Array<string> = [];

    constructor(parserOptions?: ICSVParserOptions, streamOptions?: TransformOptions) {
        super(streamOptions);

        // Override default options with given ones
        this.options = { ...defaultOptions, ...parserOptions };

        // Use headers from options if available
        if (Array.isArray(this.options.headers))
        {
            this.headers = this.options.headers;
        }

        // Check if escape string is not present in break strings, throw exception if it is      
        this.checkOptions();
    }

    // Leftover data always starts from begining of row
    // Find row ending or check if we are in flush
    // Process row

    // Transform function, called when data are available
    // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/explicit-module-boundary-types
    public _transform(chunk: any, encoding: BufferEncoding, done: TransformCallback): void
    {
        // Type of data we got should be string
        let data = String(chunk);
        // If we have leftover data from last chunk append new chunk to them
        if (this.leftoverData.length > 0) {
            data = this.leftoverData + data;
            this.leftoverData = '';
        }

        try
        {
            this.processData(data, false);
        }
        catch(err)
        {
            done(err, null);
            return;
        }
        done();
    }

    // Transform function, called after all data was delivered (at end of stream)
    public _flush(done: TransformCallback): void
    {         
        if (this.leftoverData.length > 0)
        {            
            try
            {
                this.processData(this.leftoverData, true);
            }
            catch(err)
            {
                done(err, null);
                return;
            }
        }
        done();        
    }

    // Check if escape string is not present in break strings, throw exception if it is   
    private checkOptions(): void
    {
        if (this.options.columnBreak.includes(this.options.escape))
        {
            throw new ColumnBreakError(this.options.columnBreak, this.options.escape);
        }
        if (this.options.rowBreak.includes(this.options.escape))
        {
            throw new RowBreakError(this.options.rowBreak, this.options.escape);
        }
    }

    private processColumns(colums: Array<string>): void
    {
        // Unless we got column names we will generate them / use them in first row
        if (!Array.isArray(this.options.headers) && this.firstRowParsed === false)
        {
            this.firstRowParsed = true;

            // We got headers in options
            if (this.options.headers === true)
            {
                this.headers = colums;
                return;
            }
            // We generate numbers
            else
            {
                for (let i = this.headers.length; i < colums.length; i++)
                {
                    this.headers.push(i.toString());
                }
            }
        }

        // Number of colums we parsed and number of headers we have doesnt match
        if (this.headers.length !== colums.length)
        {
            // If strict is enabled we return error instead of data
            if (this.options.strict === true)
            {
                throw new ColumnMissmatchError(colums.length,this.headers.length);
            }
            // Otherwise we add new header names (number or placeholder text)
            else if (colums.length > this.headers.length)
            {
                for (let i = this.headers.length; i < colums.length; i++)
                {
                    // We got some names, but less than number of columns
                    if (this.options.headers !== false)
                    {
                        let name = '___UNKNONW_HEADER_'+i+'___';
                        // In unlikely case this name already exists append _ till we have a unique one
                        while (this.headers.includes(name))
                        {
                            name += '_';
                        }
                        this.headers.push(name);
                    }
                    // We created numbers during first row, now we need to add some more
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

        // Publish row object
        this.push(JSON.stringify(obj));
    }

    private processData(data: string, endOfData: boolean): void
    {
        // We start at begining
        let delimiterObj: IDelimiterObject | null = {
            start: 0,
            escaped: data.startsWith(this.options.escape, 0)
        };

        // Use leftover colums
        let columns = this.leftoverColumns;
        this.leftoverColumns = [];

        let columnIndex = 0;
        let rowIndex = 0;

        // While we have start delimiter object
        while(delimiterObj !== null)
        {
            // Find end of field sequence based on if field is escaped or not
            if (delimiterObj.escaped)
            {
                columnIndex = this.findEndOfEscaping(data, delimiterObj.start, this.options.columnBreak);
                rowIndex = this.findEndOfEscaping(data, delimiterObj.start, this.options.rowBreak);
            }
            else
            {
                columnIndex = data.indexOf(this.options.columnBreak, delimiterObj.start);
                rowIndex = data.indexOf(this.options.rowBreak, delimiterObj.start);
            }

            // Set endIndex and delimiter variables to general approach for end of column and row
            let endIndex: number;
            let delimited: string;
            let endOfRow: boolean;
            
            // columnIndex was found and smaller than rowIndex
            if ((columnIndex < rowIndex && columnIndex !== -1) || (columnIndex > 0 && rowIndex === -1))
            {
                endIndex = columnIndex;
                delimited = this.options.columnBreak;
                endOfRow = false;
            }
            // rowIndex was found and smaller than columnIndex
            else if ((rowIndex < columnIndex && rowIndex !== -1) || (rowIndex > 0 && columnIndex === -1))
            {
                endIndex = rowIndex;
                delimited = this.options.rowBreak;
                endOfRow = true;
            }
            // We didnt find either
            // columIndex === rowIndex === -1
            else
            {
                // We run out of data, and need to wait for more
                if (endOfData !== true)
                {
                    this.leftoverColumns = columns;
                    this.leftoverData = data.substr(delimiterObj.start);
                    // Dont process rest of the data now
                    break;
                }
                // Called from flush, we need to process rest of the data
                else
                {
                    delimited = '';
                    endIndex = data.length - (delimiterObj.escaped ? this.options.escape.length : 0);
                    endOfRow = true;
                }
            }

            // Push in new column
            columns.push(this.extractField(data, delimiterObj, endIndex));
            
            // If we have next end create new delimiter object
            if (columnIndex !== rowIndex)
            {
                const newStart: number = endIndex + (delimiterObj.escaped ? this.options.escape.length : 0) + delimited.length;
                delimiterObj = {
                    start: newStart,
                    escaped: data.startsWith(this.options.escape, newStart) 
                };
            }
            // Otherwise set it to null to break the cycle
            else
            {
                delimiterObj = null;
            }

            // Process columns when we reach end of row
            if (endOfRow === true)
            {
                this.processColumns(columns);
                columns = [];
            }
        }
    }

    // Determine start index based on escaping and remove double escaping
    private extractField(data: string, delimiterObj: IDelimiterObject, endIndex:number): string
    {
        const startIndex = delimiterObj.start + (delimiterObj.escaped ? this.options.escape.length : 0);
        const field = data.substring(startIndex, endIndex);
        
        // TODO figure why replaceAll doesnt work
        return field.split(this.options.escape + this.options.escape).join(this.options.escape);
    }


    // We are looking for end of escaping (", or "\r\n) but only if its not escaped ("". or ""\r\n)
    private findEndOfEscaping(data: string, startIndex: number, delimiter: string) : number
    {
        let delimiterIndex: number;
            
        // Look for unding untill we run out of data
        do
        {
            delimiterIndex = data.indexOf(this.options.escape + delimiter, startIndex);
            // If we run out of data break cycle
            if (delimiterIndex === -1)
            {
                break;
            }

            let count = 0;
            let index = delimiterIndex;
            // We found escaped ending
            // Step back the data by escaping sequence and count them
            while(index > startIndex)
            {
                // We found another escape sequence
                if (data.startsWith(this.options.escape, index))
                {
                    count++;
                    // Move back one more step
                    index -= this.options.escape.length;
                }
                // We did not found escaping sequence -> break the cycle
                else
                {
                    break;
                }
            }

            // If number of escaping sequences found is odd we found end of field
            if (count % 2 === 1)
            {
                return delimiterIndex;
            }
            // If number of escaping sequences found is even, the ending we found was it-self escaped and we need to continoue looking futher
            else
            {
                startIndex = delimiterIndex + this.options.escape.length + delimiter.length - 1;
            }
        }
        while(delimiterIndex !== -1);

        return delimiterIndex;
    }
}
