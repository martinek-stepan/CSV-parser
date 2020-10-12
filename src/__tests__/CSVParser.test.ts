import {createReadStream, readFileSync, readdirSync} from 'fs';
import {join} from 'path';
import {CSVParser, ICSVRecord} from '../index';



function testCSV(name: string, parser: CSVParser, done: jest.DoneCallback): void
{
    const expectedJSON = JSON.parse(readFileSync(join(__dirname, 'json', name+'.json')).toString());
    const jsonArray: Array<ICSVRecord> = [];  
    console.time(name);
    const rs = createReadStream(join(__dirname, 'csvs', name+'.csv'));
    rs
        .on('open', ()=> {
            rs.pipe(parser)
                .on('data', (chunk: string) => {
                    jsonArray.push(JSON.parse(chunk));
                })
                .on('error', (err: Error) => {
                    console.log(err);
                    done(err);
                })
                .on('end', () => {
                    expect(jsonArray).toMatchObject(expectedJSON);
                    console.timeEnd(name);
                    done();
                });
        })
        .on('error', (err: Error) => {
            console.log(err);
            done(err);
        });
}

readdirSync(join(__dirname, 'csvs')).forEach(file => {
    const name = file.substr(0, file.length - 4);
    test(name, done => {
        const parser = new CSVParser({
            headers: true,
            escape: '"',
            rowBreak: '\n',
            columnBreak: ',',
            strict: true
        });
        testCSV(name, parser, done);

    });
});