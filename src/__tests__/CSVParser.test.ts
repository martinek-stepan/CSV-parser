import {createReadStream, readFileSync, readdirSync} from 'fs';
import {join} from 'path';
import {CSVParser, ICSVRecord} from '../index';
import {Readable} from 'stream';



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
    if (file.endsWith('.json'))
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
    if (file.endsWith('.json'))
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

test('invalid parser options', () =>
{
    expect(() => {
        new CSVParser({
            escape: 'x',
            rowBreak: 'xy'
        });
    }).toThrow(Error);

    
    expect(() => {
        new CSVParser({
            escape: 'x',
            columnBreak: 'xy'
        });
    }).toThrow(Error);
});

test('transform data leftover', done =>
{
    const parser = new CSVParser({
        headers: ['a','b']
    });
    const rs = new Readable();
    const jsonArray: Array<ICSVRecord> = [];
    rs.pipe(parser)
        .on('data', (chunk: string) => {
            jsonArray.push(JSON.parse(chunk));
        })
        .on('error', (err: Error) => {
            done(err);
        })
        .on('end', () => {
            expect(jsonArray).toMatchObject([{
                'a': 'a',
                'b': 'b'
            }]);
            done();
        });

    rs.emit('data','"a');
    rs.emit('data','",b');
    rs.emit('end');
    rs.destroy();
});


test('missing columns', done =>
{
    const parser = new CSVParser({
        headers: ['a'],
        strict: false
    });
    const rs = new Readable();
    const jsonArray: Array<ICSVRecord> = [];
    rs.pipe(parser)
        .on('data', (chunk: string) => {
            jsonArray.push(JSON.parse(chunk));
        })
        .on('error', (err: Error) => {
            done(err);
        })
        .on('end', () => {
            expect(jsonArray).toMatchObject([{
                'a': 'a',
                '___UNKNONW_HEADER_1___': 'b'
            }]);
            done();
        });

    rs.emit('data','"a');
    rs.emit('data','",b');
    rs.emit('end');
    rs.destroy();
});

test('missing columns duplicate name', done =>
{
    const parser = new CSVParser({
        headers: ['___UNKNONW_HEADER_1___'],
        strict: false
    });
    const rs = new Readable();
    const jsonArray: Array<ICSVRecord> = [];
    rs.pipe(parser)
        .on('data', (chunk: string) => {
            jsonArray.push(JSON.parse(chunk));
        })
        .on('error', (err: Error) => {
            done(err);
        })
        .on('end', () => {
            expect(jsonArray).toMatchObject([{
                '___UNKNONW_HEADER_1___': 'a',
                '___UNKNONW_HEADER_1____': 'b'
            }]);
            done();
        });

    rs.emit('data','"a');
    rs.emit('data','",b');
    rs.emit('end');
    rs.destroy();
});


test('no heeaders', done =>
{
    const parser = new CSVParser({
        headers: false,
        strict: false
    });
    const rs = new Readable();
    const jsonArray: Array<ICSVRecord> = [];
    rs.pipe(parser)
        .on('data', (chunk: string) => {
            jsonArray.push(JSON.parse(chunk));
        })
        .on('error', (err: Error) => {
            done(err);
        })
        .on('end', () => {
            expect(jsonArray).toMatchObject([{
                '0': 'a',
                '1': 'b'
            },
            {
                '0': 'x',
                '1': 'y',
                '2': 'z'
            }]);
            done();
        });

    rs.emit('data','"a');
    rs.emit('data','",b');
    rs.emit('data','\r\nx,y,z');
    rs.emit('end');
    rs.destroy();
});


test('strict header colums misscount end', done =>
{
    const parser = new CSVParser({
        headers: ['1'],
        strict: true
    });
    const rs = new Readable();
    const jsonArray: Array<ICSVRecord> = [];
    rs.pipe(parser)
        .on('data', (chunk: string) => {
            jsonArray.push(JSON.parse(chunk));
        })
        .on('error', (err: Error) => {
            expect(err).toBeInstanceOf(Error);
            done();
        })
        .on('end', () => {
            done(new Error('Expected to get error!'));
        });

    rs.emit('data','"a');
    rs.emit('data','","b"');
    rs.emit('end');
    rs.destroy();
});


test('strict header colums misscount more headers', done =>
{
    const parser = new CSVParser({
        headers: ['1','2','3'],
        strict: false
    });
    const rs = new Readable();
    const jsonArray: Array<ICSVRecord> = [];
    rs.pipe(parser)
        .on('data', (chunk: string) => {
            jsonArray.push(JSON.parse(chunk));
        })
        .on('error', (err: Error) => {
            done(err);
        })
        .on('end', () => {
            expect(jsonArray).toMatchObject([{
                '1': 'a',
                '2': 'b'
            }]);
            done();
        });

    rs.emit('data','"a');
    rs.emit('data','","b"');
    rs.emit('end');
    rs.destroy();
});

test('strict header colums misscount middle', done =>
{
    const parser = new CSVParser({
        headers: ['1'],
        strict: true
    });
    const rs = new Readable();
    const jsonArray: Array<ICSVRecord> = [];
    rs.pipe(parser)
        .on('data', (chunk: string) => {
            jsonArray.push(JSON.parse(chunk));
        })
        .on('error', (err: Error) => {
            expect(err).toBeInstanceOf(Error);
            done();
        })
        .on('end', () => {
            done(new Error('Expected to get error!'));
        });

    rs.emit('data','"a');
    rs.emit('data','",b');
    rs.emit('data','\r\nx,y');
    rs.emit('end');
    rs.destroy();
});


test('multiple escapes', done =>
{
    const parser = new CSVParser({
        headers: false,
        
    });
    const rs = new Readable();
    const jsonArray: Array<ICSVRecord> = [];
    rs.pipe(parser)
        .on('data', (chunk: string) => {
            jsonArray.push(JSON.parse(chunk));
        })
        .on('error', (err: Error) => {
            done(err);
        })
        .on('end', () => {
            expect(jsonArray).toMatchObject([{
                '0': 'a",',
                '1': 'b"",',
                '2': 'c","",'
            }]);
            done();
        });

    rs.emit('data','"a"",');
    rs.emit('data','","b"""",",');
    rs.emit('data','"c"","""","');
    rs.emit('end');
    rs.destroy();
});