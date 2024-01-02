
function visualizeImmatriculationData(csvContent) {
    let data = d3.csvParse(csvContent);
    d3.select("#immatriculation_by_brand svg").remove();
    const filter = document.getElementById("occasionNeufFilter")
    if (filter && (filter.value === 'occasion' || filter.value === 'neuf')) {
        data = data.filter(d => d.occasion === (filter === 'occasion' ? 'true' : 'false'));
    }

    const groupedData = data.reduce((acc, entry) => {
        const marque = entry.marque;
        acc[marque] = (acc[marque] || 0) + 1;
        return acc;
    }, {});

    const groupedArray = Object.entries(groupedData);

    const brands = groupedArray.map(d => d[0]);

    const xScale = d3.scaleBand()
        .domain(brands)
        .range([0, 600])
        .padding(0.2);

    const yScale = d3.scaleLinear()
        .domain([0, d3.max(groupedArray, d => d[1])])
        .range([250, 0]);

    const colorScale = d3.scaleOrdinal(d3.schemeCategory10);

    const svg = d3.select("#immatriculation_by_brand")
        .append("svg")
        .attr("width", 800)
        .attr("height", 300);

    svg.selectAll(".bar")
        .data(groupedArray)
        .enter().append("rect")
        .attr("class", "bar")
        .attr("x", d => xScale(d[0]))
        .attr("width", xScale.bandwidth())
        .attr("y", d => yScale(d[1]))
        .attr("height", d => 250 - yScale(d[1]))
        .attr("fill", d => colorScale(d[0]));

    svg.append("g")
        .attr("transform", "translate(0,250)")
        .call(d3.axisBottom(xScale))
        .selectAll("text")
        .attr("transform", "rotate(-45)")
        .style("text-anchor", "end")
        .text("Marque");

    svg.append("g")
        .call(d3.axisLeft(yScale));

    svg.selectAll(".bar-label")
        .data(groupedArray)
        .enter().append("text")
        .attr("class", "bar-label")
        .attr("x", d => xScale(d[0]) + xScale.bandwidth() / 2)
        .attr("y", d => yScale(d[1]) - 5)
        .style("text-anchor", "middle")
        .style("font-size", "12px")
        .text(d => d[1]);
}

function visualizeImmatriculationPuissanceData(csvContent) {
    d3.select("#immatriculation_puissance svg").remove();

    let data = d3.csvParse(csvContent);
console.log(data)
    const filter = document.getElementById("occasionNeufFilter")
    if (filter && (filter.value === 'occasion' || filter.value === 'neuf')) {
        data = data.filter(d => d.occasion === (filter === 'occasion' ? "true" : "false"));
    }

    const groupedData = d3.nest()
        .key(d => d.puissance)
        .rollup(values => values.map(d => ({ immatriculation: d.immatriculation, couleur: d.couleur })))
        .entries(data);

    const groupedArray = groupedData.map(d => ({
        puissance: +d.key,
        immatriculations: d.value
    }));

    const colorScale = d3.scaleOrdinal(d3.schemeCategory10);
    const puissanceImmatriculationsSvg = d3.select("#immatriculation_puissance")
        .append("svg")
        .attr("width", 800)
        .attr("height", 300);
    const immatriculationsXScale = d3.scaleLinear()
        .domain([0, d3.max(groupedArray, d => d.immatriculations.length)])
        .range([0, 600]);

    const puissanceYScale = d3.scaleBand()
        .domain(groupedArray.map(d => d.puissance))
        .range([0, 250])
        .padding(0.1);

    puissanceImmatriculationsSvg.selectAll("rect")
        .data(groupedArray)
        .enter().append("rect")
        .attr("y", d => puissanceYScale(d.puissance))
        .attr("width", d => immatriculationsXScale(d.immatriculations.length))
        .attr("height", puissanceYScale.bandwidth())
        .attr("fill", d => colorScale(d.puissance));

    puissanceImmatriculationsSvg.append("g")
        .attr("transform", "translate(0,250)")
        .call(d3.axisBottom(immatriculationsXScale));

    puissanceImmatriculationsSvg.append("g")
        .call(d3.axisLeft(puissanceYScale));

    puissanceImmatriculationsSvg.append("text")
        .attr("transform", "translate(400,290)")
        .style("text-anchor", "middle")
        .text("Immatriculations");

    puissanceImmatriculationsSvg.append("text")
        .attr("transform", "rotate(-90)")
        .attr("y", -40)
        .attr("dy", "0.71em")
        .style("text-anchor", "end")
        .text("Puissance");

    puissanceImmatriculationsSvg.selectAll(".bar-label")
        .data(groupedArray)
        .enter().append("text")
        .attr("class", "bar-label")
        .attr("x", d => immatriculationsXScale(d.immatriculations.length))
        .attr("y", d => puissanceYScale(d.puissance) + puissanceYScale.bandwidth() / 2)
        .attr("dx", 5)
        .attr("dy", "0.35em")
        .style("font-size", "12px")
        .text(d => `Puissance: ${d.puissance}`);
}
