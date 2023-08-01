const PROCESS_EVENTS_URL = '/process/events';
const DOWNLOAD_URL = '/download';

const downloadBtn = document.getElementById('downloadBtn');
downloadBtn.onclick = ev => {
    const id = window.sessionStorage.getItem('id');
    window.open(`${DOWNLOAD_URL}?id=${id}`, '_blank');
}

const processAnim = document.getElementById('processAnim');
const successAnim = document.getElementById('successAnim');
const failureAnim = document.getElementById('failureAnim');
const successText = document.getElementById('successText');
const failureText = document.getElementById('failureText');

class Stepper {
    constructor(totalNumberOfCircles) {
        this.circleNumber = 1;
        this.lineNumber = this.circleNumber - 1;
        this.totalNumberOfCircles = totalNumberOfCircles;
        this.totalNumberOfLines = this.totalNumberOfCircles - 1;
    }

    start() {
        const circle = document.getElementById(`circle${this.circleNumber}`);
        const spinner = circle.getElementsByClassName('spinner')[0];
        spinner.classList.remove('d-none');
    }

    next(hasPreviousStepFailed = false) {
        let circle = document.getElementById(`circle${this.circleNumber}`);
        let spinner = circle.getElementsByClassName('spinner')[0];
        spinner.classList.add('d-none');

        if (!hasPreviousStepFailed) {
            const successAnim = circle.getElementsByClassName('success-anim')[0];
            successAnim.classList.remove('d-none');
            successAnim.play();

            this.circleNumber++;
            this.lineNumber++;

            circle = document.getElementById(`circle${this.circleNumber}`);
            spinner = circle.getElementsByClassName('spinner')[0];
            spinner.classList.remove('d-none');

            const line = document.getElementById(`line${this.lineNumber}`);
            line.classList.add('line-success');
        }
        else {
            const failureAnim = circle.getElementsByClassName('failure-anim')[0];
            failureAnim.classList.remove('d-none');
            failureAnim.play();
        }
    }

    stop(hasPreviousStepFailed = false) {
        const circle = document.getElementById(`circle${this.circleNumber}`);
        const spinner = circle.getElementsByClassName('spinner')[0];
        spinner.classList.add('d-none');

        if (!hasPreviousStepFailed) {
            const successAnim = circle.getElementsByClassName('success-anim')[0];
            successAnim.classList.remove('d-none');
            successAnim.play();
        }
        else {
            const failureAnim = circle.getElementsByClassName('failure-anim')[0];
            failureAnim.classList.remove('d-none');
            failureAnim.play();
        }
    }
}


const stepper = new Stepper(4);
const logs = document.getElementById('logs');

const id = window.sessionStorage.getItem('id');
const oldPrimaryKeyIdx = window.sessionStorage.getItem('oldPrimaryKeyIdx');
const newPrimaryKeyIdx = window.sessionStorage.getItem('newPrimaryKeyIdx');

const url = `${PROCESS_EVENTS_URL}?id=${id}&oldPrimaryKeyIdx=${oldPrimaryKeyIdx}&newPrimaryKeyIdx=${newPrimaryKeyIdx}`;
const eventSource = new EventSource(url);
let hasErrorOccurred = false;
eventSource.onmessage = ev => {
    const data = JSON.parse(ev.data);
    if (data.error) {
        hasErrorOccurred = true;
        stepper.next(true);
        eventSource.close();
        logs.innerHTML += `<p class="text-danger">${data.error}</p>`;
        processAnim.pause();
        processAnim.classList.add('d-none');
        failureText.classList.remove('d-none');
        failureAnim.classList.remove('d-none');
        failureAnim.play();
    }
    else if (data.start) {
        logs.innerHTML += `<p class="text-info">${data.start}</p>`;
        stepper.start();
    }
    else if (data.step) {
        stepper.next();
    }
    else if (data.stop) {
        logs.innerHTML += `<p class="text-info">${data.stop}</p>`;
        stepper.stop();
    }
    else if (data.message) {
        logs.innerHTML += `<p>${data.message}</p>`;
    }
    else if (data.closeConnection) {
        console.log('closing......')
        eventSource.close();
        processAnim.classList.add('d-none');
        processAnim.pause();
        processAnim.classList.add('d-none');
        if (!hasErrorOccurred) {
            downloadBtn.disabled = false;
            successText.classList.remove('d-none');
            successAnim.classList.remove('d-none');
            successAnim.play();
        }
        else {
            failureText.classList.remove('d-none');
            failureAnim.classList.remove('d-none');
            failureAnim.play();
        }
    }
}

eventSource.onerror = ev => {
    hasErrorOccurred = true;
    eventSource.close();
}
