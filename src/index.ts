/* eslint-disable @typescript-eslint/explicit-module-boundary-types */
/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable @typescript-eslint/no-non-null-assertion */
import { Transform, TransformCallback, TransformOptions } from 'stream';


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

const defaultOptions: ICSVParserOptions = {
    headers: true,
    escape: '"',
    rowBreak: '\r\n',
    columnBreak: ',',
    strict: true
};

export interface ICSVRecord
{
    [key: string]: string
}

export class CSVParser extends Transform
{
    private options: ICSVParserOptions;
    private leftoverData?: string;
    private headers: Array<string> = [];
    private firstRowParsed = false;
    private leftoverColumns: Array<string> = [];

    constructor(parserOptions?: ICSVParserOptions, streamOptions?: TransformOptions) {
        super(streamOptions);

        this.options = { ...defaultOptions, ...parserOptions };

        if (Array.isArray(this.options.headers!))
        {
            this.headers = this.options.headers!;
        }

        // Check if escape string is not present in break strings, throw exception if it is      
        this.checkOptions();
    }

    // Leftover data always starts from begining of row
    // Find row ending or check if we are in flush
    // Process row

    // Transform function, called when data are available
    public _transform(chunk: any, encoding: BufferEncoding, done: TransformCallback): void
    {
        // Type of data we got should be string
        let data = String(chunk);
        // If we have leftover data from last chunk append new chunk to them
        if (this.leftoverData) {
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
        if (this.leftoverData)
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
        if (this.options.columnBreak?.includes(this.options.escape!))
        {
            throw new Error('Column break ['+this.options.columnBreak!+'] can not include, escape string ['+this.options.escape!+']!');
        }
        if (this.options.rowBreak?.includes(this.options.escape!))
        {
            throw new Error('Row break ['+this.options.rowBreak!+'] can not include, escape string ['+this.options.escape!+']!');
        }
    }

    private processColumns(colums: Array<string>): void
    {
        if (!Array.isArray(this.options.headers) && this.firstRowParsed === false)
        {
            this.firstRowParsed = true;

            if (this.options.headers === true)
            {
                this.headers = colums;
                return;
            }
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
                throw new Error('Number of colums('+colums.length+') does not match number of headers('+this.headers.length+')!');
            }
            // Otherwise we add new header names (number or placeholder text)
            else if (colums.length > this.headers.length)
            {
                for (let i = this.headers.length; i < colums.length; i++)
                {
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

        this.push(JSON.stringify(obj));
    }

    private processData(data: string, endOfData: boolean): void
    {
        // We start at begining
        let delimiterObj: any = {
            start: 0,
            escaped: data.startsWith(this.options.escape!, 0)
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
            // TODO escapedescape
            if (delimiterObj.escaped)
            {
                columnIndex = this.findEndOfEscaping(data, delimiterObj.start, this.options.columnBreak!);
                rowIndex = this.findEndOfEscaping(data, delimiterObj.start, this.options.rowBreak!);
                //columnIndex = data.indexOf(this.options.escape! + this.options.columnBreak!, delimiterObj.start);
                //rowIndex = data.indexOf(this.options.escape! + this.options.rowBreak!, delimiterObj.start);
            }
            else
            {
                columnIndex = data.indexOf(this.options.columnBreak!, delimiterObj.start);
                rowIndex = data.indexOf(this.options.rowBreak!, delimiterObj.start);
            }

            // Set endIndex and delimiter variables to general approach for end of column and row
            let endIndex: number;
            let delimited: string;
            let endOfRow: boolean;
            
            // columnIndex was found and smaller than rowIndex
            if ((columnIndex < rowIndex && columnIndex !== -1) || (columnIndex > 0 && rowIndex === -1))
            {
                endIndex = columnIndex;
                delimited = this.options.columnBreak!;
                endOfRow = false;
            }
            // rowIndex was found and smaller than columnIndex
            else if ((rowIndex < columnIndex && rowIndex !== -1) || (rowIndex > 0 && columnIndex === -1))
            {
                endIndex = rowIndex;
                delimited = this.options.rowBreak!;
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
                    endIndex = data.length - (delimiterObj.escaped ? this.options.escape!.length : 0);
                    endOfRow = true;
                }
            }

            // Push in new column
            columns.push(this.extractField(data, delimiterObj, endIndex));
            
            // If we have next end create new delimiter object
            if (columnIndex !== rowIndex)
            {
                const newStart = endIndex + (delimiterObj.escaped ? this.options.escape!.length : 0) + delimited.length;
                delimiterObj = {
                    start: newStart,
                    escaped: data.startsWith(this.options.escape!, newStart) 
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

    private extractField(data: string, delimiterObj: any, endIndex:number): string
    {
        const startIndex = delimiterObj.start + (delimiterObj.escaped ? this.options.escape!.length : 0);
        const field = data.substring(startIndex, endIndex);
        
        // TODO figure why replaceAll doesnt work
        return field.split(this.options.escape! + this.options.escape!).join(this.options.escape!);
    }


    // We are looking for end of escaping (", or "\r\n) but only if its not escaped ("". or ""\r\n)
    private findEndOfEscaping(data: string, startIndex: number, delimiter: string) : number
    {
        let delimiterIndex: number;
            
        do
        {
            delimiterIndex = data.indexOf(this.options.escape! + delimiter, startIndex);
            if (delimiterIndex === -1)
            {
                break;
            }

            let count = 0;
            let index = delimiterIndex;
            while(index > startIndex)
            {
                if (data.startsWith(this.options.escape!, index))
                {
                    count++;
                    index -= this.options.escape!.length;
                }
                else
                {
                    break;
                }
            }

            if (count % 2 === 1)
            {
                return delimiterIndex;
            }
            else
            {
                startIndex = delimiterIndex + this.options.escape!.length + delimiter.length -1;
            }
        }
        while(delimiterIndex !== -1);

        return delimiterIndex;
    }
}
