/* eslint-disable quotes */
import {createReadStream, readFileSync, readdirSync} from 'fs';
import {join} from 'path';
import {CSVParser, ICSVRecord} from '../index';



function testCSV(name: string, parser: CSVParser, done: jest.DoneCallback): void
{
    const expectedJSON = JSON.parse(readFileSync(join(__dirname, 'data', name+'.json')).toString());
    const jsonArray: Array<ICSVRecord> = [];
    const rs = createReadStream(join(__dirname, 'data', name+'.csv'));
    rs
        .on('open', ()=> {
            rs.pipe(parser)
                .on('data', (chunk: string) => {
                    jsonArray.push(JSON.parse(chunk));
                })
                .on('error', (err: Error) => {
                    done(err);
                })
                .on('end', () => {
                    expect(jsonArray).toMatchObject(expectedJSON);
                    done();
                });
        })
        .on('error', (err: Error) => {
            console.log(err);
            done(err);
        });
}

readdirSync(join(__dirname, 'data', 'rfc4180')).forEach(file => {
    if (file.endsWith(".json"))
    {
        return;
    }
    const name = 'rfc4180/'+file.substr(0, file.length - 4);
    test(name, done => {
        const parser = new CSVParser();
        testCSV(name, parser, done);

    });
});

readdirSync(join(__dirname, 'data', 'customized')).forEach(file => {
    if (file.endsWith(".json"))
    {
        return;
    }

    const name = 'customized/'+file.substr(0, file.length - 4);
    test(name, done => {
        const parser = new CSVParser({
            headers: true,
            escape: '\'',
            rowBreak: '\n',
            columnBreak: ';',
            strict: true
        });
        testCSV(name, parser, done);

    });
});