const UPLOAD_URL = '/upload';

const uploadForm = document.getElementById('uploadForm');
uploadForm.onsubmit = ev => {
    return false;
}

document.getElementById('uploadBtn').onclick = ev => {

    const uploadBody = document.getElementById('uploadBody');

    const oldFileInput = document.getElementById('oldFileInput');
    const newFileInput = document.getElementById('newFileInput');
    const oldFileProgressBar = document.getElementById('oldFileProgressBar');
    const newFileProgressBar = document.getElementById('newFileProgressBar');

    const uploadSuccessSection = document.getElementById('uploadSuccessSection');
    const uploadSuccessAnim = document.getElementById('uploadSuccessAnim');

    const uploadFailureSection = document.getElementById('uploadFailureSection');
    const uploadFailureAnim = document.getElementById('uploadFailureAnim');

    const unsupportedFileSection = document.getElementById('unsupportedFileSection');
    const unsupportedFileAnim = document.getElementById('unsupportedFileAnim');

    if (oldFileInput.files.length > 0 && newFileInput.files.length > 0) {
        const oldFile = oldFileInput.files[0];
        const newFile = newFileInput.files[0];

        const formData = new FormData();
        formData.append('oldFile', oldFile);
        formData.append('newFile', newFile);

        const xhr = new XMLHttpRequest();
        xhr.open('POST', UPLOAD_URL, true);

        oldFileProgressBar.parentElement.classList.remove('d-none');
        newFileProgressBar.parentElement.classList.remove('d-none');

        xhr.upload.onprogress = ev => {
            if (ev.lengthComputable) {
                const percentComplete = ((ev.loaded / ev.total) * 100).toFixed(2);
                oldFileProgressBar.style.width = `${percentComplete}%`;
                oldFileProgressBar.innerText = `${percentComplete}%`;
                newFileProgressBar.style.width = `${percentComplete}%`;
                newFileProgressBar.innerText = `${percentComplete}%`;
            }
        }

        xhr.onreadystatechange = ev => {
            if (xhr.readyState === 4 && xhr.status === 200) {
                uploadBody.classList.add('d-none');
                uploadSuccessSection.classList.remove('d-none');
                uploadSuccessAnim.play();
                setTimeout(() => {
                    const response = JSON.parse(xhr.response);
                    window.sessionStorage.setItem('id', response.id);
                    window.location = `/preview?id=${response.id}`;
                }, 2500);
            }
            else if (xhr.readyState === 4 && xhr.status === 401) {
                uploadBody.classList.add('d-none');
                unsupportedFileSection.classList.remove('d-none');
                unsupportedFileAnim.play();
            }
            else if (xhr.readyState === 4 && xhr.status !== 200) {
                uploadBody.classList.add('d-none');
                uploadFailureSection.classList.remove('d-none');
                uploadFailureAnim.play();
            }
        }

        xhr.send(formData);
    }
}

