function visualizeCatalogueData(content) {
    console.log("here")
    const data = d3.csvParse(content);
    console.log("pie data loaded :", data);
    // Création d'un objet pour stocker les quantités de chaque marque
    const quantitiesByBrand = {};

    // Agrégation des quantités pour chaque marque
    data.forEach(function (entry) {
      const brand = entry.marque;
      if (!quantitiesByBrand[brand]) {
        quantitiesByBrand[brand] = 0;
      }
      quantitiesByBrand[brand]++;
    });

    const occasionsData = data.filter((d) => d.occasion === "true");
    // Créer une nouvelle structure pour les quantités de chaque marque pour les voitures d'occasion
    const quantitiesByBrandOccasion = {};
    occasionsData.forEach(function (entry) {
      const brand = entry.marque;
      if (!quantitiesByBrandOccasion[brand]) {
        quantitiesByBrandOccasion[brand] = 0;
      }
      quantitiesByBrandOccasion[brand]++;
    });

    const width = 600;
    const height = 400;
    const radius = Math.min(width, height) / 2;

    const svg = d3
      .select("#cars_by_brand")
      .append("svg")
      .attr("width", width + 100) // add marge for legend
      .attr("height", height + 75) // add marge for description
      .append("g")
      .attr("transform", `translate(${width / 2},${height / 2})`);

    const color = d3.scaleOrdinal(d3.schemePaired);

    const pie = d3.pie().value((d) => d.value);

    const dataForPie = Object.entries(quantitiesByBrand).map(
      ([key, value]) => ({ marque: key, value: (value / data.length) * 100 })
    );

    const tooltip = d3.select("#tooltip");

    const arc = d3.arc().innerRadius(0).outerRadius(radius);

    const arcs = svg
      .selectAll("arc")
      .data(pie(dataForPie))
      .enter()
      .append("g")
      .attr("class", "arc");

    // Segment
    arcs
      .append("path")
      .attr("d", arc)
      .attr("fill", (d) => color(d.data.marque))
      .attr("stroke", "white")
      .style("stroke-width", "2px")
      // Nom de chaque marque sur chaque segment // Position du texte au centre de chaque segment

      // Interaction au survol pour les infobulles
      .on("mouseover", function (event, d) {
        d3.select(this).attr("opacity", 0.7); // Rendre le segment un peu plus clair

        brand = dataForPie[d];

        tooltip
          .html(`${brand.marque}: ${brand.value.toFixed(2)}%`)
          .style("opacity", 1)
          .style("left", event.pageX + 10 + "px")
          .style("top", event.pageY - 20 + "px");
      })
      .on("mouseout", function () {
        d3.select(this).attr("opacity", 1); // Rétablir l'opacité normale

        tooltip.style("opacity", 0); // Masquer l'infobulle lorsqu'on quitte le segment
      });
    // Création de l'élément titre pour la description
    svg
      .append("text")
      .attr("x", 0)
      .attr("y", height / 2 + 50)
      .attr("text-anchor", "middle")
      .text("Repartition des marques dans le catalogue");

    const legend = svg
      .selectAll("g.legend")
      .data(pie(dataForPie))
      .enter()
      .append("g")
      .attr("class", "legend")
      .attr(
        "transform",
        (d, i) =>
          `translate(${width / 2.5},${-height / 2.15 + i * dataForPie.length})`
      );

    // Ajout des carrés colorés pour chaque marque dans la légende
    legend
      .append("rect")
      .attr("width", 10)
      .attr("height", 10)
      .attr("fill", (d) => color(d.data.marque));

    // Ajout du texte pour le nom de la marque dans la légende
    legend
      .append("text")
      .attr("x", 15)
      .attr("y", 10)
      .attr("text-anchor", "start")
      .style("font-size", "12px")
      .text((d) => d.data.marque);

    const filterSelect = d3
      .select("#filterSelect")
      .append("select")
      .on("change", updateChart);

    filterSelect
      .selectAll("option")
      .data(["Tout", "Voitures d'occasion", "Voitures neuves"])
      .enter()
      .append("option")
      .text((d) => d);

    // Créer un slider pour filtrer par puissance
    const slider = d3
      .select("#filterPower")
      .append("input")
      .attr("type", "range")
      .attr("min", 0)
      .attr("max", 500) // Exemple de plage de puissance, ajustez selon vos besoins
      .attr("value", 500)
      .on("input", updateChart);

    // Créer un élément texte pour afficher la valeur courante du slider
    const sliderValueText = d3
      .select("#filterPower") // Sélectionnez l'élément où vous avez créé le slider
      .append("text")
      .attr("x", width / 2) // Ajustez la position x selon votre mise en page
      .attr("y", height + 20) // Ajustez la position y selon votre mise en page
      .attr("text-anchor", "middle")
      .style("font-size", "14px");
    // Mise à jour de la valeur du texte en fonction du slider
    slider.on("input", function () {
      const selectedPower = slider.property("value");
      sliderValueText.text(`Puissance cible ou inferieur à ${selectedPower}`); // Mettez à jour le texte avec la valeur actuelle du slider
      updateChart();
    });

    function updateChart() {
      const selectedPower = slider.property("value");

      // Filtrer les données en fonction de la puissance sélectionnée
      const dataOnPower = data.filter((d) => +d.puissance <= selectedPower);

      const selectedOption = filterSelect.property("value");
      let filteredData = dataOnPower;

      if (selectedOption === "Voitures d'occasion") {
        filteredData = filteredData.filter((d) => d.occasion === "true");
      } else if (selectedOption === "Voitures neuves") {
        filteredData = filteredData.filter((d) => d.occasion === "false");
      }
      const quantitiesByBrand = {};

      filteredData.forEach(function (entry) {
        const brand = entry.marque;
        if (!quantitiesByBrand[brand]) {
          quantitiesByBrand[brand] = 0;
        }
        quantitiesByBrand[brand]++;
      });

      const dataForPie = Object.entries(quantitiesByBrand).map(
        ([key, value]) => ({
          marque: key,
          value: (value / filteredData.length) * 100,
        })
      );

      // Supprimer les anciens arcs et leurs groupes associés
      svg.selectAll("g.arc").remove();

      // Ajouter de nouveaux groupes arcs basés sur les données filtrées
      const arcs = svg
        .selectAll("arc")
        .data(pie(dataForPie))
        .enter()
        .append("g")
        .attr("class", "arc");

      arcs
        .append("path")
        .attr("d", arc)
        .attr("fill", (d) => color(d.data.marque))
        .attr("stroke", "white")
        .style("stroke-width", "2px")
        // Mouseover and Mouseout events for tooltip
        .on("mouseover", function (event, d) {
          d3.select(this).attr("opacity", 0.7); // Rendre le segment un peu plus clair

          brand = dataForPie[d];

          tooltip
            .html(`${brand.marque}: ${brand.value.toFixed(2)}%`)
            .style("opacity", 1)
            .style("left", event.pageX + 10 + "px")
            .style("top", event.pageY - 20 + "px");
        })
        .on("mouseout", function () {
          d3.select(this).attr("opacity", 1); // Rétablir l'opacité normale

          tooltip.style("opacity", 0); // Masquer l'infobulle lorsqu'on quitte le segment
        });
      arcs.exit().remove();

      // Supprimer l'ancienne légende
      svg.selectAll("g.legend").remove();

      // Recréer la légende avec les données filtrées
      const legend = svg
        .selectAll("g.legend")
        .data(pie(dataForPie))
        .enter()
        .append("g")
        .attr("class", "legend")
        .attr(
          "transform",
          (d, i) =>
            `translate(${width / 2.5},${
              -height / 2.15 + i * dataForPie.length + Math.min(30, 10 * i)
            })`
        );

      // Ajout des carrés colorés pour chaque marque dans la légende
      legend
        .append("rect")
        .attr("width", 10)
        .attr("height", 10)
        .attr("fill", (d) => color(d.data.marque));

      // Ajout du texte pour le nom de la marque dans la légende
      legend
        .append("text")
        .attr("x", 15)
        .attr("y", 10)
        .attr("text-anchor", "start")
        .style("font-size", "12px")
        .text((d) => d.data.marque);

      // Recalculer les statistiques nécessaires pour le boxplot avec les données filtrées
      const pricesByBrand = {};
      filteredData.forEach(function (entry) {
        const brand = entry.marque;
        const price = parseFloat(entry.prix.replace(",", "")); // Convertir le prix en nombre
        const name = entry.nom;
        if (!pricesByBrand[brand]) {
          pricesByBrand[brand] = [];
        }
        pricesByBrand[brand].push({ price, name });
      });

      // Formatage des données pour le diagramme en boîte
      const boxplotData = Object.keys(pricesByBrand).map((brand) => {
        return {
          marque: brand,
          prix: pricesByBrand[brand].map((p) => p.price),
          nom: pricesByBrand[brand].map((n) => n.name),
        };
      });

      // Transition pour supprimer l'ancienne échelle yScale

      // Mettre à jour l'échelle x en fonction des prix max et min des nouvelles données
      xScale.domain([0, d3.max(boxplotData, (d) => d3.max(d.prix))]);
      svgBox
        .select("#boxplot > svg > g > g:nth-child(1)")
        .transition()
        .duration(500)
        .call(d3.axisBottom(xScale));

      // Supprimer les anciens rectangles et points
      svgBox.selectAll("rect").remove();
      svgBox.selectAll(".point-group").remove();

      // Recréer les rectangles pour chaque marque (boxplot)
      svgBox
        .selectAll("boxes")
        .data(boxplotData)
        .enter()
        .append("rect")
        .attr("x", (d) => xScale(d3.min(d.prix)))
        .attr("width", (d) => xScale(d3.max(d.prix)) - xScale(d3.min(d.prix)))
        .attr("y", (d) => yScale(d.marque))
        .attr("height", yScale.bandwidth())
        .attr("stroke", "black")
        .style("fill", "#69b3a2") // Couleur grise
        .style("opacity", 0.3); // Opacité ajustée à 0.3

      // Ajout des points pour chaque prix par marque avec la couleur spécifique
      svgBox
        .selectAll(".point")
        .data(boxplotData)
        .enter()
        .append("g")
        .attr("class", "point-group")
        .selectAll("circle")
        .data((d) =>
          d.prix.map((p, i) => ({ marque: d.marque, price: p, nom: d.nom[i] }))
        )
        .enter()
        .append("circle")
        .attr("class", "point")
        .attr("cx", (d) => xScale(d.price))
        .attr("cy", (d) => yScale(d.marque) + yScale.bandwidth() / 2)
        .attr("r", 4) // Taille des points
        .style("fill", (d) => colorScale(d.marque)) // Appliquer la couleur spécifique à chaque marque
        .attr("stroke", "black")
        .on("mouseover", mouseover)
        .on("mousemove", mousemove)
        .on("mouseleave", mouseleave);
    }

    ////////////////////////////////

    // Création de l'objet pour regrouper les prix par marque
    const pricesByBrand = {};
    data.forEach(function (entry) {
      const brand = entry.marque;
      const nom = entry.nom;

      const prix = parseFloat(entry.prix.replace(",", "")); // Convertir le prix en nombre
      if (!pricesByBrand[brand]) {
        pricesByBrand[brand] = [];
      }
      pricesByBrand[brand].push({ name: nom, price: prix });
    });

    // Formatage des données pour le diagramme en boîte
    const boxplotData = Object.keys(pricesByBrand).map((brand) => {
      return {
        marque: brand,
        prix: pricesByBrand[brand].map((p) => p.price),
        nom: pricesByBrand[brand].map((n) => n.name),
      };
    });

    // Dimensions et marges du graphique
    const margin = { top: 10, right: 10, bottom: 70, left: 70 };
    //const width = 460 - margin.left - margin.right;
    //const height = 400 - margin.top - margin.bottom;

    // Sélection de l'élément SVG où dessiner le graphique
    const svgBox = d3
      .select("#boxplot")
      .append("svg")
      .attr("width", width + margin.left + margin.right)
      .attr("height", height + margin.top + margin.bottom)
      .append("g")
      .attr("transform", `translate(${margin.left},${margin.top})`);

    // Création de l'échelle en fonction de l'axe y (prix)
    const yScale = d3
      .scaleBand()
      .domain(boxplotData.map((d) => d.marque))
      .range([height, 0]) // Ajuster la plage pour l'axe y
      .padding(0.1);

    const xScale = d3
      .scaleLinear()
      .domain([0, d3.max(boxplotData, (d) => d3.max(d.prix))])
      .range([0, width]) // Ajuster la plage pour l'axe x
      .nice();

    // Ajout de l'axe des marques (x)
    svgBox
      .append("g")
      .attr("transform", `translate(0,${height})`)
      .call(d3.axisBottom(xScale))
      .selectAll("text")
      .attr("transform", "rotate(-90)")
      .attr("x", -10)
      .attr("y", 0)
      .attr("dy", ".35em")
      .style("text-anchor", "end");

    // Ajout de l'axe des prix (y)
    svgBox.append("g").call(d3.axisLeft(yScale));

    // Couleur
    const colorScale = d3
      .scaleOrdinal()
      .domain(boxplotData.map((d) => d.marque))
      .range(
        d3.quantize(
          (t) => d3.interpolateInferno(t * 0.8 + 0.1),
          boxplotData.length
        )
      );

    // Ajout des boîtes pour chaque marque
    svgBox
      .selectAll("boxes")
      .data(boxplotData)
      .enter()
      .append("rect")
      .attr("x", (d) => xScale(d3.min(d.prix)))
      .attr("width", (d) => xScale(d3.max(d.prix)) - xScale(d3.min(d.prix)))
      .attr("y", (d) => yScale(d.marque))
      .attr("height", yScale.bandwidth())
      .attr("stroke", "black")
      .style("fill", "#69b3a2") // Couleur grise
      .style("opacity", 0.3); // Opacité ajustée à 0.3

    var tooltip2 = d3
      .select("#boxplot")
      .append("div")
      .style("opacity", 0)
      .attr("class", "tooltip2")
      .style("font-size", "16px");
    // Three function that change the tooltip when user hover / move / leave a cell
    var mouseover = function (d) {
      tooltip2.transition().duration(200).style("opacity", 1);
      tooltip2
        .html("<span style='color:white'>" + d.nom + ": " + d.price + "</span>") // + d.Prior_disorder + "<br>" + "HR: " +  d.HR)
        .style("left", d3.mouse(this)[0] + 80 + "px")
        .style("top", d3.mouse(this)[1] - height - 35 + "px");
    };
    var mousemove = function (d) {
      tooltip2
        .style("left", d3.mouse(this)[0] + 80 + "px")
        .style("top", d3.mouse(this)[1] - height - 35 + "px");
    };
    var mouseleave = function (d) {
      tooltip2.transition().duration(200).style("opacity", 0);
    };

    // Ajout des points pour chaque prix par marque avec la couleur spécifique
    svgBox
      .selectAll(".point")
      .data(boxplotData)
      .enter()
      .append("g")
      .attr("class", "point-group")
      .selectAll("circle")
      .data((d) =>
        d.prix.map((p, i) => ({ marque: d.marque, price: p, nom: d.nom[i] }))
      )
      .enter()
      .append("circle")
      .attr("class", "point")
      .attr("cx", (d) => xScale(d.price))
      .attr("cy", (d) => yScale(d.marque) + yScale.bandwidth() / 2)
      .attr("r", 4) // Taille des points
      .style("fill", (d) => colorScale(d.marque)) // Appliquer la couleur spécifique à chaque marque
      .attr("stroke", "black")
      .on("mouseover", mouseover)
      .on("mousemove", mousemove)
      .on("mouseleave", mouseleave);
}
