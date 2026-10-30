// Own exactly two children; never discover or terminate an unrelated server.
import {spawn,spawnSync} from 'node:child_process';
import net from 'node:net';
import path from 'node:path';
import {fileURLToPath} from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const children = [];
let stopping = false;
const exited = child => child.exitCode!==null||child.signalCode!==null;
async function stopOwnedBroker() {
  for(const [script,variable] of [['postgres-local.sh','CONNECTIVITY_LAUNCHER_POSTGRES_PID'],['kafka-local.sh','CONNECTIVITY_LAUNCHER_BROKER_PID']]) {
    const expected=Number(process.env[variable]||0);
    if(!expected)continue;
    const lifecycle=path.join(root,'scripts',script);
    const status=spawnSync(lifecycle,['status'],{cwd:root,encoding:'utf8'});
    if(status.status===0&&JSON.parse(status.stdout).pid===expected) {
      await new Promise(resolve=>{const task=spawn(lifecycle,['stop'],{cwd:root,stdio:'inherit'});task.once('exit',resolve);task.once('error',resolve);});
    }
  }
}
async function requireFree(port) {
  const listener = net.createServer();
  await new Promise((resolve,reject)=>listener.once('error',reject).listen(port,'127.0.0.1',resolve));
  await new Promise(resolve=>listener.close(resolve));
}
async function stop(code=0) {
  if(stopping)return;
  stopping=true;
  for(const child of children)if(!exited(child)&&!child.killed)child.kill('SIGTERM');
  await Promise.all(children.map(child=>exited(child)?Promise.resolve():new Promise(resolve=>{
    const timer=setTimeout(()=>{if(!exited(child))child.kill('SIGKILL');resolve();},8000);
    child.once('exit',()=>{clearTimeout(timer);resolve();});
  })));
  await stopOwnedBroker();
  process.exitCode=code;
}
try {
  await requireFree(3000);
  await requireFree(5173);
  for(const [entry,args,env,cwd] of [
    ['server/dist/index.js',[],{PORT:'3000',HOST:'127.0.0.1'},root],
    ['frontend/node_modules/vite/bin/vite.js',['--host','127.0.0.1','--port','5173','--strictPort'],{},path.join(root,'frontend')],
  ]) {
    const child=spawn(process.execPath,[path.join(root,entry),...args],{cwd,env:{...process.env,...env},stdio:'inherit'});
    children.push(child);
    child.on('error',error=>{console.error(error.message);void stop(1);});
    child.on('exit',code=>{if(!stopping){console.error(`${entry} exited (${code??'signal'}); stopping owned services.`);void stop(code||1);}});
  }
  console.log('Connectivity Credits: http://127.0.0.1:5173 · Express API/built app: http://127.0.0.1:3000 · Ctrl+C to stop');
  process.on('SIGINT',()=>void stop(130));
  process.on('SIGTERM',()=>void stop(143));
} catch(error) {
  console.error(`Cannot launch: ${error.message}. Existing services were left untouched.`);
  await stop(1);
}
