function visualizeClientsData(content) {
  
    const data = d3.csvParse(content);
    // Affichage des données pour vérification
    console.log("clients: ", data);

    let filteredData = data.map((d) => {
      d.sexe = d.sexe !== "M" && d.sexe !== "F" ? "A" : d.sexe;
      return d;
    });
    // Dimensions du graphique
    const width = 800;
    const height = 800;
    const margin = { top: 20, right: 20, bottom: 40, left: 40 };

    // Création de l'élément SVG
    const svg = d3.select("#nuageVisu").append("svg");

    // Echelles pour les axes X et Y
    const xScale = d3
      .scaleLinear()
      .domain([18, d3.max(filteredData, (d) => +d.age)]) // Convertir en nombre avec "+" pour assurer la conversion
      .range([margin.left, width * 2 - margin.right]);

    const yScale = d3
      .scaleLinear()
      .domain([-150, d3.max(filteredData, (d) => +d.taux)]) // Convertir en nombre avec "+" pour assurer la conversion
      .range([height * 1.5 - margin.bottom, margin.top]);

    // Ajout de l'axe des marques (x)
    svg
      .append("g")
      .attr("transform", `translate(0,${height})`)
      .call(d3.axisBottom(xScale).ticks(20).tickFormat(d3.format("d")))
      .selectAll("text")
      .attr("transform", "rotate(-90)")
      .attr("x", -10)
      .attr("y", 0)
      .attr("dy", ".35em")
      .style("text-anchor", "end");
    // Ajout de l'axe des prix (y)
    svg
      .append("g")
      .attr("transform", `translate(${margin.left},0)`)
      .call(d3.axisLeft(yScale));

    // Ajout des cercles pour représenter les individus

    svg
      .selectAll("circle")
      .data(filteredData.filter((d) => d["2eme voiture"] === "false"))
      .enter()
      .append("circle")
      .attr("class", "data-circle")
      .attr("cx", (d) => xScale(+d.age)) // Convertir en nombre avec "+" pour assurer la conversion
      .attr("cy", (d) => yScale(+d.taux)) // Convertir en nombre avec "+" pour assurer la conversion
      .attr("r", (d) => {
        if (+d.nbEnfantsAcharge > 2) {
          return 6;
        } else if (+d.nbEnfantsAcharge > 0) {
          return 8;
        } else {
          return 12;
        }
      }) // Rayon du cercle
      .attr("fill", (d) =>
        d.sexe === "M" ? "blue" : d.sexe === "F" ? "pink" : "green"
      ) // Couleur en fonction du sexe
      .attr("opacity", 0.7) // Opacité
      .attr("stroke", "black") // Bordure
      .attr("stroke-width", 1) // Largeur de la bordure
      // Interaction : survol des cercles pour afficher les informations
      // Interaction : survol des rectangles pour afficher les informations
      .on("mouseover", function (d) {
        d3.select(this).attr("fill", "red"); // Changement de couleur au survol
      })
      .on("mouseout", function (d) {
        d3.select(this).attr("fill", (d) =>
          d.sexe === "M" ? "blue" : d.sexe === "F" ? "pink" : "green"
        ); // Retour à la couleur d'origine
      })
      .append("title")
      .text(
        (d) =>
          `Age: ${d.age}, Sexe: ${d.sexe}, Taux: ${
            d.taux
          }, Situation familiale: ${d.situationFamiliale}, Enfants à charge: ${
            d.nbEnfantsAcharge
          }, Deuxième voiture: ${
            d["2eme voiture"] ? "Oui" : "Non"
          }, Immatriculation: ${d.immatriculation}`
      );

    svg
      .selectAll("rect")
      .data(filteredData.filter((d) => d["2eme voiture"] === "true"))
      .enter()
      .append("rect")
      .attr("class", "data-rect")
      .attr("x", (d) => xScale(+d.age) - 6) // Ajustement pour centrer le carré
      .attr("y", (d) => yScale(+d.taux) - 6) // Ajustement pour centrer le carré
      .attr("width", (d) => {
        if (+d.nbEnfantsAcharge > 2) {
          return 6;
        } else if (+d.nbEnfantsAcharge > 0) {
          return 8;
        } else {
          return 12;
        }
      }) // Largeur du carré
      .attr("height", (d) => {
        if (+d.nbEnfantsAcharge > 2) {
          return 6;
        } else if (+d.nbEnfantsAcharge > 0) {
          return 8;
        } else {
          return 12;
        }
      })
      .attr("fill", (d) =>
        d.sexe === "M" ? "blue" : d.sexe === "F" ? "pink" : "green"
      ) // Couleur en fonction du sexe
      .attr("opacity", 0.7) // Opacité
      .attr("stroke", "black") // Bordure
      .attr("stroke-width", 1) // Largeur de la bordure
      // Interaction : survol des rectangles pour afficher les informations
      .on("mouseover", function (d) {
        d3.select(this).attr("fill", "red"); // Changement de couleur au survol
      })
      .on("mouseout", function (d) {
        d3.select(this).attr("fill", (d) =>
          d.sexe === "M" ? "blue" : d.sexe === "F" ? "pink" : "green"
        ); // Retour à la couleur d'origine
      })
      .append("title")
      .text(
        (d) =>
          `Age: ${d.age}, Sexe: ${d.sexe}, Taux: ${
            d.taux
          }, Situation familiale: ${d.situationFamiliale}, Enfants à charge: ${
            d.nbEnfantsAcharge
          }, Deuxième voiture: ${
            d["2eme voiture"] ? "Oui" : "Non"
          }, Immatriculation: ${d.immatriculation}`
      );

    // Calculate the bounding box based on circles and rectangles
    const bbox = svg.node().getBBox();

    // Set SVG's width and height based on the bounding box
    svg.attr("width", bbox.width).attr("height", bbox.height);

    // Ajout d'une légende
    const legend = svg
      .append("g")
      .attr("class", "legend")
      .attr("transform", `translate(${2.25 * width - 100},${height - 400})`); // Position de la légende à 2 * width

    // Ajout des cercles pour la légende
    legend
      .selectAll("circle")
      .data(["M", "F", "Autre"]) // Exemple de catégories pour la légende
      .enter()
      .append("circle")
      .attr("cx", 0)
      .attr("cy", (d, i) => i * 20)
      .attr("r", 5)
      .attr("fill", (d) => (d === "M" ? "blue" : d === "F" ? "pink" : "green"));

    // Ajout des textes pour la légende
    legend
      .selectAll("text")
      .data(["Homme", "Femme", "Autre"]) // Exemple de texte pour la légende
      .enter()
      .append("text")
      .attr("x", 10)
      .attr("y", (d, i) => i * 20)
      .text((d) => d)
      .attr("alignment-baseline", "middle");

    // Ajout de la légende pour la taille des ronds/carrés
    legend
      .append("circle")
      .attr("cx", 0)
      .attr("cy", 60)
      .attr("r", 6)
      .attr("fill", "none")
      .attr("stroke", "black")
      .attr("stroke-width", 1);

    legend
      .append("circle")
      .attr("cx", 0)
      .attr("cy", 70)
      .attr("r", 8)
      .attr("fill", "none")
      .attr("stroke", "black")
      .attr("stroke-width", 1);

    legend
      .append("circle")
      .attr("cx", 0)
      .attr("cy", 80)
      .attr("r", 12)
      .attr("fill", "none")
      .attr("stroke", "black")
      .attr("stroke-width", 1);

    legend
      .append("text")
      .attr("x", 10)
      .attr("y", 72)
      .text("Individus ayant 3+/1-2/0 enfants")
      .attr("alignment-baseline", "middle");

    legend
      .append("rect")
      .attr("x", -4)
      .attr("y", 110)
      .attr("width", 9)
      .attr("height", 9)
      .attr("fill", "none")
      .attr("stroke", "black")
      .attr("stroke-width", 1);

    legend
      .append("text")
      .attr("x", 10)
      .attr("y", 117)
      .text("Individus ayant une 2e voiture")
      .attr("alignment-baseline", "middle");

    // Créer un slider pour l'âge
    const ageSlider = d3
      .select("#filterAge")
      .append("input")
      .attr("type", "range")
      .attr("min", 18)
      .attr(
        "max",
        d3.max(filteredData, (d) => +d.age)
      )
      .attr(
        "value",
        d3.max(filteredData, (d) => +d.age)
      )
      .on("input", updateGraph);

    // Créer un slider pour le taux
    const tauxSlider = d3
      .select("#filterTaux")
      .append("input")
      .attr("type", "range")
      .attr("min", -150)
      .attr(
        "max",
        d3.max(filteredData, (d) => +d.taux)
      )
      .attr(
        "value",
        d3.max(filteredData, (d) => +d.taux)
      )
      .on("input", updateGraph);

    // Créer un menu déroulant pour la situation familiale
    const situationFamilialeOptions = [
      "Tous",
      "Celibataire",
      "En couple",
      "Marie",
      "Divorce",
    ];
    const situationFamilialeMenu = d3
      .select("#filterSituation")
      .append("select")
      .on("change", updateGraph);

    situationFamilialeMenu
      .selectAll("option")
      .data(situationFamilialeOptions)
      .enter()
      .append("option")
      .attr("value", (d) => d)
      .text((d) => d);

    function updateGraph() {
      const ageLimit = ageSlider.property("value");
      const tauxLimit = tauxSlider.property("value");
      const situLimit = situationFamilialeMenu.property("value");

      // Filtrer les données en fonction de l'âge
      let filtered = data.filter((d) => {
        let letter =
          situLimit === "Celibataire"
            ? "C"
            : situLimit === "En couple"
            ? "E"
            : situLimit === "Marie"
            ? "M"
            : situLimit === "Divorce"
            ? "D"
            : "T";
        return (
          +d.age <= ageLimit &&
          +d.taux <= tauxLimit &&
          (letter === "T" ||
            (situLimit !== "T" && d.situationFamiliale.startsWith(letter)))
        );
      });

      // Mettre à jour l'échelle x en fonction des prix max et min des nouvelles données
      xScale.domain([18, d3.max(filtered, (d) => +d.age)]);

      svg
        .select("#nuageVisu > svg > g:nth-child(1)")
        .transition()
        .duration(500)
        .call(d3.axisBottom(xScale));

      yScale.domain([-150, d3.max(filtered, (d) => +d.taux)]);
      svg
        .select("#nuageVisu > svg > g:nth-child(2)")
        .transition()
        .duration(500)
        .call(d3.axisLeft(yScale));

      svg.selectAll(".data-circle").remove();
      svg.selectAll(".data-rect").remove();

      svg
        .selectAll("circle")
        .data(filtered.filter((d) => d["2eme voiture"] === "false"))
        .enter()
        .append("circle")
        .attr("class", "data-circle")
        .attr("cx", (d) => xScale(+d.age)) // Convertir en nombre avec "+" pour assurer la conversion
        .attr("cy", (d) => yScale(+d.taux)) // Convertir en nombre avec "+" pour assurer la conversion
        .attr("r", (d) => {
          if (+d.nbEnfantsAcharge > 2) {
            return 6;
          } else if (+d.nbEnfantsAcharge > 0) {
            return 8;
          } else {
            return 12;
          }
        }) // Rayon du cercle
        .attr("fill", (d) =>
          d.sexe === "M" ? "blue" : d.sexe === "F" ? "pink" : "green"
        ) // Couleur en fonction du sexe
        .attr("opacity", 0.7) // Opacité
        .attr("stroke", "black") // Bordure
        .attr("stroke-width", 1) // Largeur de la bordure
        // Interaction : survol des rectangles pour afficher les informations
        .on("mouseover", function (d) {
          d3.select(this).attr("fill", "red"); // Changement de couleur au survol
        })
        .on("mouseout", function (d) {
          d3.select(this).attr("fill", (d) =>
            d.sexe === "M" ? "blue" : d.sexe === "F" ? "pink" : "green"
          ); // Retour à la couleur d'origine
        })
        .append("title")
        .text(
          (d) =>
            `Age: ${d.age}, Sexe: ${d.sexe}, Taux: ${
              d.taux
            }, Situation familiale: ${
              d.situationFamiliale
            }, Enfants à charge: ${d.nbEnfantsAcharge}, Deuxième voiture: ${
              d["2eme voiture"] ? "Oui" : "Non"
            }, Immatriculation: ${d.immatriculation}`
        );

      svg
        .selectAll("rect")
        .data(filtered.filter((d) => d["2eme voiture"] === "true"))
        .enter()
        .append("rect")
        .attr("class", "data-rect")
        .attr("x", (d) => xScale(+d.age) - 6) // Ajustement pour centrer le carré
        .attr("y", (d) => yScale(+d.taux) - 6) // Ajustement pour centrer le carré
        .attr("width", (d) => {
          if (+d.nbEnfantsAcharge > 2) {
            return 6;
          } else if (+d.nbEnfantsAcharge > 0) {
            return 8;
          } else {
            return 12;
          }
        }) // Largeur du carré
        .attr("height", (d) => {
          if (+d.nbEnfantsAcharge > 2) {
            return 6;
          } else if (+d.nbEnfantsAcharge > 0) {
            return 8;
          } else {
            return 12;
          }
        })
        .attr("fill", (d) =>
          d.sexe === "M" ? "blue" : d.sexe === "F" ? "pink" : "green"
        ) // Couleur en fonction du sexe
        .attr("opacity", 0.7) // Opacité
        .attr("stroke", "black") // Bordure
        .attr("stroke-width", 1) // Largeur de la bordure
        // Interaction : survol des rectangles pour afficher les informations
        .on("mouseover", function (d) {
          d3.select(this).attr("fill", "red"); // Changement de couleur au survol
        })
        .on("mouseout", function (d) {
          d3.select(this).attr("fill", (d) =>
            d.sexe === "M" ? "blue" : d.sexe === "F" ? "pink" : "green"
          ); // Retour à la couleur d'origine
        })
        .append("title")
        .text(
          (d) =>
            `Age: ${d.age}, Sexe: ${d.sexe}, Taux: ${
              d.taux
            }, Situation familiale: ${
              d.situationFamiliale
            }, Enfants à charge: ${d.nbEnfantsAcharge}, Deuxième voiture: ${
              d["2eme voiture"] ? "Oui" : "Non"
            }, Immatriculation: ${d.immatriculation}`
        );
    }
}
