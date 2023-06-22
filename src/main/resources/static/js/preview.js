const oldTable = document.getElementById("oldTable");
const newTable = document.getElementById("newTable");
const oldPrimaryKey = document.getElementById("oldPrimaryKey");
const newPrimaryKey = document.getElementById("newPrimaryKey");


let oldTableColIdx = 0;
let newTableColIdx  = 0;

const initialSetup = (table, newColIdx, colorClass) => {
    // Color column header
    const headerThElement = table.getElementsByTagName('tr')
        .item(0).getElementsByTagName('th');
    headerThElement.item(newColIdx).classList.add(colorClass);

    // Color column rows
    Array.from(table.getElementsByTagName('tr')).slice(1)
        .forEach(node => {
            const tdElement = node.getElementsByTagName('td');
            tdElement.item(newColIdx).classList.add(colorClass);
        });
}

const addColorToColumn = (table, oldColIdx, newColIdx, colorClass) => {
    // Color column header
    const headerThElement = table.getElementsByTagName('tr')
        .item(0).getElementsByTagName('th');
    headerThElement.item(oldColIdx).classList.remove(colorClass);
    headerThElement.item(newColIdx).classList.add(colorClass);

    // Color column rows
    Array.from(table.getElementsByTagName('tr')).slice(1)
        .forEach(node => {
            const tdElement = node.getElementsByTagName('td');
            tdElement.item(oldColIdx).classList.remove(colorClass);
            tdElement.item(newColIdx).classList.add(colorClass);
        });
}

oldPrimaryKey.onchange = ev => {
    const newColIdx = ev.target.value;
    addColorToColumn(oldTable, oldTableColIdx, newColIdx, 'bg-warning');
    oldTableColIdx = newColIdx;
}
newPrimaryKey.onchange = ev => {
    const newColIdx = ev.target.value;
    addColorToColumn(newTable, newTableColIdx, newColIdx, 'bg-info');
    newTableColIdx = newColIdx;
}

initialSetup(oldTable, oldTableColIdx, 'bg-warning');
initialSetup(newTable, newTableColIdx, 'bg-info');